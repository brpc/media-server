// Media Server - Host or proxy live multimedia streams.
// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Ge,Jun (jge666@gmail.com)
//          Jiashun Zhu(zhujiashun2010@gmail.com)

#ifndef MEDIA_SERVER_TS_QUEUE_H
#define MEDIA_SERVER_TS_QUEUE_H

#include "butil/containers/bounded_queue.h"
#include "brpc/ts.h"
#include "brpc/rtmp.h"
#include "brpc/progressive_reader.h"
#include "brpc/describable.h"
#include "butil/third_party/murmurhash3/murmurhash3.h"
#include "event_log_manager.h"
#include "shared_map.h"

DECLARE_int64(ts_duration_ms);
DECLARE_int64(ts_max_stored);

class TsEntry;
struct TsEntryKey {
    std::string key;
    int64_t seq;
    bool started_with_keyframe;
    int64_t expiration_ms;
};
struct HashTsEntryKey {
    size_t operator()(const TsEntryKey& key) const;
};
typedef SharedMap<TsEntryKey, TsEntry, HashTsEntryKey> TsEntryMap;
inline TsEntryMap* get_ts_entry_map() {
    return butil::get_leaky_singleton<TsEntryMap>();
}

class TsQueue;
struct TsQueueKey {
    std::string key;
    bool started_with_keyframe;
    int64_t ts_duration_ms;
    int64_t ts_num_per_m3u8;
};
struct HashTsQueueKey {
    size_t operator()(const TsQueueKey& key) const;
};
typedef SharedMap<TsQueueKey, TsQueue, HashTsQueueKey> TsQueueMap;
inline TsQueueMap* get_ts_queue_map() {
    return butil::get_leaky_singleton<TsQueueMap>();
}

inline int64_t make_duetime(const int64_t now_ms, const int64_t expiration_ms) {
    return now_ms + expiration_ms;
}

inline int64_t get_stored_ts_num(int64_t ts_num_per_m3u8) {
    return std::max(ts_num_per_m3u8, FLAGS_ts_max_stored);
}

static const int64_t SEQUENCE_NUMBER_INIT = 1;

// The identifier of a HLS user (playing a m3u8)
typedef uint64_t HLSUserId;

// Geneate an id for a HLS user.
HLSUserId generate_hls_user_id();

// Meta info of a HLS user.
struct HLSUserMeta : public brpc::SharedObject {
    HLSUserId user_id;

    // Seq number of the first TS to be downloaded.
    int64_t start_seq_num;

    // TTL in milliseconds.
    int64_t duetime_ms;
    int64_t expiration_ms;
    
    explicit HLSUserMeta(HLSUserId user_id2)
        : user_id(user_id2)
        , start_seq_num(SEQUENCE_NUMBER_INIT - 1)
        , duetime_ms(-1)
        , expiration_ms(-1) {
    }

    // Refresh TTL.
    void mark_as_alive()
    { duetime_ms = make_duetime(butil::gettimeofday_ms(), expiration_ms); }
};
// Get the meta object associated with the user_id
// Returns true on found, false otherwise.
bool get_hls_user_meta(
    HLSUserId user_id, const int64_t expiration_ms, butil::intrusive_ptr<HLSUserMeta>* out);
// Get the meta object associated with the user_id. If the object does not
// exist, create one.
// Returns true on *creation*, false otherwise.
bool get_or_new_hls_user_meta(
    HLSUserId user_id, const int64_t expiration_ms, butil::intrusive_ptr<HLSUserMeta>* out);

// Represent a TS downloading connection.
class TsDownloader : public brpc::SharedObject {
public:
    // Take over the attachment.
    TsDownloader(brpc::ProgressiveAttachment* pa);
    TsDownloader(brpc::ProgressiveAttachment* pa,
                 const butil::IOBuf& unsent_data,
                 bool is_internal,
                 const std::string& key);
    ~TsDownloader();
    
    // Send the data to the downloader. The data is strictly sent after all
    // data sent before.
    int send(const butil::IOBuf& data) { return append_or_flush(&data); }
    
    // Flush unsent data.
    int flush() { return append_or_flush(NULL); }
    
    bool has_unsent() const { return !_unsent.empty(); }

    butil::EndPoint remote_side() const
    { return _pa ? _pa->remote_side() : butil::EndPoint(); }

    void remove_pa();
    
private:
friend class TsEntry;
friend class FlushDownloadersThread;
    DISALLOW_COPY_AND_ASSIGN(TsDownloader);

    int append_or_flush(const butil::IOBuf* data);

    // The attachment for downloading TS progressively. Destroyed in dtor.
    butil::intrusive_ptr<brpc::ProgressiveAttachment> _pa;
    
    // Internally used by FlushDownloadersThread and TsEntry::append_ts_data
    // to mark downloader as deleted without allocating another container.
    bool _to_be_removed;

    // Set to true before _pa is reset to NULL.
    bool _pa_removed;

    // TS data which temporarily fails to be written into the attachment.
    butil::IOBuf _unsent;

    EventLogManager _event_log_manager;
    size_t _sent_bytes;
    std::string _key;
    const bool _is_internal;
};

// Containing one TS.
class TsEntry : public brpc::SharedObject {
public:
    explicit TsEntry(const std::string& key, int64_t seq_num);
    explicit TsEntry(const TsEntryKey& key_and_seq_num);

    ~TsEntry();

    // [Thread-safe] Add the attachment as a downloader of the TS data in this
    // entry. If the TS was complete, the attachment will be written with the
    // full data immediately. If the write is successful, the attachment is
    // Destroy()-ed, otherwise the attachment is moved to a background thread
    // for periodic retrying of writing. If the TS is not complete yet, the
    // attachment will be written with existing data immediately and put
    // into a list of downloaders for later data.
    void add_downloader(brpc::ProgressiveAttachment* pa, 
                        bool is_internal,
                        const std::string& charge_key);

    // [Thread-safe] Must be called before calling append_ts_data.
    int begin_writing(int64_t start_timestamp, bool is_keyframe);

    // [Not thread-safe] This method can only be called by one writer.
    // Append TS data into this entry.
    int append_ts_data(const butil::IOBuf& data, bool count_one_keyframe);
    
    // [Thread-safe] Complete this entry.
    void end_writing(int64_t end_timestamp);

    // The realtime that after which this TsEntry will be unaccessible and removed.
    bool is_complete() const { return _end_timestamp >= 0; }
    bool is_begined() const { return _start_timestamp >= 0; }
    int64_t discontinity_counter() const { return _discontinuity_counter; }
    bool is_started_with_keyframe() const { return _started_with_keyframe; }
    int64_t start_timestamp() const { return _start_timestamp; }
    int64_t end_timestamp() const { return _end_timestamp; }
    size_t size() const { return _ts.size(); }
    int64_t seq_num() const { return _seq_num; }
    const std::string& key() const { return _key; }
    int keyframe_count() const { return _nkf; }
    bool is_idle_more_than(int64_t idle_ms, int64_t now_ms) const;
    int64_t expiration_ms() const {return _expiration_ms; }

private:
friend  std::ostream& operator<<(std::ostream& os, const TsEntry& ts);
    DISALLOW_COPY_AND_ASSIGN(TsEntry);

    void remove_downloader(const TsDownloader*);
    static void remove_downloader_static(TsEntry* ts_entry,
                                         TsDownloader* downloader);

    mutable pthread_mutex_t _mutex;
    std::string _key;
    int64_t _seq_num;
    bool _started_with_keyframe;
    int _nkf;
    int64_t _discontinuity_counter;
    butil::IOBuf _ts;
    int64_t _start_timestamp;
    int64_t _end_timestamp;
    int64_t _last_active_real_ms;
    int64_t _expiration_ms;
    std::vector<butil::intrusive_ptr<TsDownloader> > _downloaders;
    // For unique usage of writer.
    std::vector<butil::intrusive_ptr<TsDownloader> > _tmp_downloaders;
};

// Emulate a RTMP stream to convert a RTMP stream to a bounded queue of TS.
class TsQueue : public brpc::RtmpStreamBase,
                public brpc::Describable {
public:
    TsQueue(const TsQueueKey& key);
    ~TsQueue();
    
    // @RtmpStreamBase
    int SendMetaData(const brpc::RtmpMetaData& metadata, const butil::StringPiece& name);
    int SendAudioMessage(const brpc::RtmpAudioMessage& msg);
    int SendVideoMessage(const brpc::RtmpVideoMessage& msg);
    int SendStopMessage(const butil::StringPiece& error_description);
    butil::EndPoint remote_side() const;
    butil::EndPoint local_side() const;

    bool get_ts_entry(int64_t seq_num, butil::intrusive_ptr<TsEntry>* ptr);
    bool get_or_new_ts_entry(int64_t seq_num, butil::intrusive_ptr<TsEntry>* ptr);

    // Generate a media playlist containing TS like this:
    //   stream_name.seq<seq_num>.ts
    // Returns 0 on success, -1 otherwise.
    int generate_media_playlist(std::ostream& os, int64_t* start_seq_num,
                                const butil::StringPiece& stream_name,
                                const butil::StringPiece& queries);

    // @Describable
    void Describe(std::ostream& os, const brpc::DescribeOptions&) const;

    const std::string& key() const { return _key; }

    // Register a callback which is called when this TsQueue becomes idle
    // (generate_media_playlist is not called for a long time)
    // This function can only be called ONCE.
    void on_idle(google::protobuf::Closure* callback);

    bool disable_on_idle(int64_t now_ms);
 
private:
friend void create_remove_idle_queue_thread();
    DISALLOW_COPY_AND_ASSIGN(TsQueue);

    void write_next();
    void remove_oldest_entry(butil::intrusive_ptr<TsEntry>* oldest_entry);
    int get_or_move_to_next_ts_entry(butil::intrusive_ptr<TsEntry>* out,
                                     int64_t timestamp, bool is_keyframe);
    static void* remove_idle_queue_thread(void*);

    std::string _key;  // for removal/debugging
    int64_t _first_avc_seq_header_delay;  // for monitoring
    int64_t _first_video_message_delay;   // ^
    int64_t _first_aac_seq_header_delay;  // ^
    int64_t _first_audio_message_delay;   // ^
    butil::IOBuf _ts_data;                 // storing TS transcoded from RTMP
    brpc::TsWriter _ts_writer;      // the transcoder
    mutable pthread_mutex_t _mutex;
    int64_t _first_seq_num;
    int64_t _write_seq_num;
    int64_t _ts_duration_ms;
    int64_t _ts_num_per_m3u8;
    int64_t _duetime_ms;  // updated on each call to generate_media_playlist
    int _nts_started_with_kf;
    butil::BoundedQueue<butil::intrusive_ptr<TsEntry> > _ts_entry_list;
    butil::atomic<google::protobuf::Closure*> _on_idle_callback;
    bool _started_with_keyframe_in_ts;
};

// Get the TsQueue associated with the key.
// Returns true on found, false otherwise.
bool get_ts_queue(const TsQueueKey& key, butil::intrusive_ptr<TsQueue>* ptr);
// Get the TsQueue associated with the key, if the queue does not exist, create
// a new one.
// Returns true on *creation*, false otherwise.
bool get_or_new_ts_queue(
    const TsQueueKey& key, butil::intrusive_ptr<TsQueue>* ptr);

#endif // MEDIA_SERVER_TS_QUEUE_H
