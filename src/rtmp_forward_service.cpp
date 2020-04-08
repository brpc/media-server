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

#include <gflags/gflags.h>
#include <memory>                           // std::shared_ptr
#include "bthread/bthread.h"
#include "bvar/bvar.h"
#include "butil/endpoint.h"
#include "butil/containers/flat_map.h"
#include "butil/containers/pooled_map.h"
#include "brpc/builtin/common.h"
#include "brpc/channel.h"
#include "brpc/server.h"
#include "brpc/describable.h"
#include "brpc/policy/hasher.h"              // MurmurHash32V
#include "brpc/socket_id.h"
#include "brpc/socket.h"
#include "brpc/redis.h"
#include "frame_queue.h"
#include "rtmp_forward_service.h"
#include "http_streaming_service.h"
#include "util.h"
#include "event_log_manager.h"

DECLARE_string(proxy_to);
DECLARE_string(proxy_lb);

DEFINE_int32(retry_interval_ms, 1000, "Milliseconds between consecutive retries");
DEFINE_int32(max_retry_duration_ms, 10000, "Retry for at most so many seconds");
DEFINE_int32(fast_retry_count, 2, "Number of retries w/o any delays");
DEFINE_int32(connect_timeout_ms, -1, "Timeout for establish TCP connections, "
             "Set to timeout_ms/3 by default (-1)");
DEFINE_int32(timeout_ms, 1000, "Timeout for creating streams");
DEFINE_string(default_vhost, "_", "Default vhost for tcUrl without ?vhost=xxx");
DEFINE_bool(show_streams_in_vars, false, "show stats of streams in bvar");
DEFINE_int32(dump_stats_interval, -1, "Print stats of all streams to log every "
             "so many seconds. <=0 means don't dump.");
DEFINE_bool(unified_play_connections, false, "Share connections amongst all "
            "playing streams, requiring the server-side supports specifying "
            "app in stream_name");
DEFINE_int32(keep_pulling_seconds, 0, "Keep pulling rtmp streams for so "
             "many seconds even if all players quit");
DEFINE_string(original_timestamp_domains, "", "domains kept with original timestamp");
DEFINE_string(keep_duration_domains, "", "domains kept duration in metadata");
DEFINE_bool(stop_kickedoff_publisher, false, "Send stopping messages to old "
            "publishers kicked off by new publishers");
DEFINE_int32(timestamp_offset, 0, "offset for timestamps");
DEFINE_bool(simplified_rtmp_play, false, "Indicate whether to use simplified rtmp protocol to play");
DEFINE_string(default_app, "bdms_default_app", "Default app for the case when app field in url misses");
DEFINE_bool(reject_second_stream_when_republish, false, "When set, reject the second "
            "publish stream and terminate the second publish connection when republish");
DEFINE_bool(log_stats, false, "When set, write STATS logs about players and publishers");

#define SRS_DEFAULT_VHOST   "__defaultVhost__"
#define RTMP_UNIFIED_APP    "unified_app"
// Use default vhost of SRS as the unified domain which will be converted to
// FLAGS_default_vhost by media-servers being online before support for
// -unified_play_connections.
#define RTMP_UNIFIED_TCURL  "rtmp://" SRS_DEFAULT_VHOST "/" RTMP_UNIFIED_APP

static void print_media_server_revision(std::ostream& os, void*) {
#if defined(MEDIA_SERVER_REVISION)
    os << MEDIA_SERVER_REVISION;
#else
    os << "undefined";
#endif
}

static bvar::PassiveStatus<std::string> s_media_server_revision(
    "media_server_revision", print_media_server_revision, NULL);

static bvar::Adder<int64_t> g_rtmp_user_count("rtmp_user_count");

const brpc::RtmpPublishType RTMP_PUBLISH_FAKE = (brpc::RtmpPublishType)-1;

RtmpForwardServiceOptions::RtmpForwardServiceOptions()
    : port(-1)
    , internal_port(-1)
    , share_play_connection(true)
    , share_publish_connection(true)
    , internal_service(false) {
}

// Saving all proxying clients.
static pthread_mutex_t s_proxy_client_map_mutex = PTHREAD_MUTEX_INITIALIZER;
typedef butil::PooledMap<std::string, brpc::RtmpClient> ProxyClientMap;
static ProxyClientMap* s_proxy_client_map = NULL;

// Associated with one player.
struct PlayerInfo : public brpc::SharedObject {
    // The stream to send audio/video/metadata.
    butil::intrusive_ptr<brpc::RtmpStreamBase> stream;

    // Iterate PlayerGroup.frame_queue.
    FrameCursor* cursor;

    // Save sent metadata/headers to avoid resending same stuff again.
    std::shared_ptr<brpc::RtmpMetaData> sent_metadata;
    std::shared_ptr<brpc::RtmpVideoMessage> sent_avc_seq_header;
    std::shared_ptr<brpc::RtmpAudioMessage> sent_aac_seq_header;
    
    // stats
    size_t sent_video_messages;
    size_t sent_audio_messages;
    size_t sent_user_messages;
    size_t stuck_video_times; // "stuck" = temporarily fail to send
    size_t stuck_audio_times;
    size_t stuck_user_times;
    size_t sent_video_bytes;
    size_t sent_audio_bytes;
    size_t sent_video_frames;

    bool keep_original_time;
    // Indicated whether this player is at the outtest cdn,
    // used for keep ingestion
    bool is_outtest_cdn;

    PlayerInfo();
    ~PlayerInfo();

    // Write frames got from `cursor' into the stream.
    void flush();
    
    // Reset the player as if it's just added.
    void reset();

private:
    DISALLOW_COPY_AND_ASSIGN(PlayerInfo);
friend class RtmpForwardService;
    // Used by RtmpForwardService.dump_stats() only.
    size_t stuck_video_times_stored_by_dump_stats;
    size_t stuck_audio_times_stored_by_dump_stats;

    // used to control timestamp in metadata
    bool _reset_base_timestamp;
    uint32_t _base_timestamp;
    uint32_t _last_timestamp;
};

class PlayerAsPublisher;
class RtmpForwarder;

template <typename T, size_t N>
class AverageOfMultipleSeconds {
public:
    AverageOfMultipleSeconds() : _index(0) { }
    void add(T t) { _data[(_index++) % N] = t; }
    T get() const {
        const uint32_t saved_index = _index;
        if (saved_index >= N) {
            T sum = _data[0];
            for (uint32_t i = 1; i < N; ++i) {
                sum += _data[i];
            }
            return sum / N;
        } else if (saved_index == 0) {
            return T();
        } else {
            T sum = _data[0];
            for (uint32_t i = 1; i < saved_index; ++i) {
                sum += _data[i];
            }
            return sum / saved_index;
        }
    }
private:
    uint32_t _index;
    T _data[N];
};

// Group of players to the same stream.
class PlayerGroup : public brpc::SharedObject {
public:
    PlayerGroup(const std::string& key,
                int64_t launcher_create_time,
                int64_t first_play_or_publish_delay);
    ~PlayerGroup();

    // Get all players inside.
    void list_all(std::vector<butil::intrusive_ptr<PlayerInfo> >* list_out);
    
    // Get #players.
    size_t count();

    // Push one frame into the frame_queue.
    // Returns true on successfully queued, false otherwise.
    bool queue(Frame& frame, const void* pusher);

    // Must be called every second to update temporal stats.
    void update_stats_every_second();

    // Manipulating reference inside, not trivial. Save the result if needed.
    butil::EndPoint remote_side() const;

    int64_t start_idle_time_ms() const { return _start_idle_time_ms; }

private:
friend class MonitoringServiceImpl;
friend class RtmpForwardService;
    DISALLOW_COPY_AND_ASSIGN(PlayerGroup);
    static void OnCheckPuller(void*);

    // Make sure ops to members thread-safe.
    pthread_mutex_t _member_mutex;

    // list of targets to broadcast audio/video messages.
    std::vector<butil::intrusive_ptr<PlayerInfo> > _list;

    // pull stream from the target server in proxy mode.
    PlayerAsPublisher* _puller;

    // set to true when puller is being created, false otherwise.
    bool _creating_puller;

    // the server-side stream for publishing data.
    butil::intrusive_ptr<RtmpForwarder> _publisher;

public:
    // Cached video/audio messages which will be sent to newly added players
    // immediately.
    FrameQueue frame_queue;
    
private:
    // Recorded time when player is empty, 0 = not idle
    // other = the time when the latest player leaves,
    // it's used for keeping stream when all players quit,
    // another usage is postponing stopping transcoding.
    int64_t _start_idle_time_ms;

    // true if puller is destroyed
    bool _has_removed;

    std::string _key;
    size_t _last_keyframe_index_in_video_frames;
    size_t _last_gop_size;
    size_t _video_bytes;
    size_t _video_frames;
    size_t _audio_bytes;
    size_t _audio_frames;
    size_t _stored_video_bytes;
    size_t _stored_video_frames;
    size_t _stored_audio_bytes;
    size_t _stored_audio_frames;
    uint32_t _video_bytes_last_second;
    AverageOfMultipleSeconds<float, 10> _video_frames_second;
    uint32_t _audio_bytes_last_second;
    uint32_t _audio_frames_last_second;

    int64_t _launcher_create_realtime_us;
    int64_t _first_play_or_publish_delay;
    int64_t _create_realtime_us;

    struct Bvars {
        bvar::Adder<size_t> video_bytes_total;
        bvar::PerSecond<bvar::Adder<size_t> > video_bytes_second;
        bvar::Adder<size_t> audio_bytes_total;
        bvar::PerSecond<bvar::Adder<size_t> > audio_bytes_second;
        explicit Bvars(const std::string& key)
            : video_bytes_second(key, "video_bytes_second", &video_bytes_total)
            , audio_bytes_second(key, "audio_bytes_second", &audio_bytes_total) {
        }
    };
    Bvars* _bvars;
};

// Currently the inheritance only plays the role of renaming.
class PublishProxy : public brpc::RtmpRetryingClientStream {
public:
    PublishProxy(const std::string& key, RtmpForwarder* publisher);
    ~PublishProxy();
    void OnStop();
    // @RtmpRetryingClientStream
    int SendVideoMessage(const brpc::RtmpVideoMessage& msg);
    int SendAudioMessage(const brpc::RtmpAudioMessage& msg);
    int SendMetaData(const brpc::RtmpMetaData& metadata, const butil::StringPiece& name);
private:

    std::string _key;
    butil::intrusive_ptr<RtmpForwarder> _publisher; 
};

class ForwardProxy : public brpc::RtmpRetryingClientStream {
};

// Server-side streams, corresponding to client-side publishers or players.
class RtmpForwarder : public brpc::RtmpServerStream,
                      public brpc::Describable {
public:
    RtmpForwarder(RtmpForwardService* service,
                  const brpc::RtmpConnectRequest& conn_req,
                  bool is_rtmp_user);
    ~RtmpForwarder();

    // @RtmpServerStream
    void OnStop();
    void OnPlay(const brpc::RtmpPlayOptions& opt,
                butil::Status* status,
                google::protobuf::Closure* done);
    void OnPublish(const std::string& stream_name,
                   brpc::RtmpPublishType type,
                   butil::Status* status,
                   google::protobuf::Closure* done);
    void OnUserData(void*);
    void OnMetaData(brpc::RtmpMetaData* metadata, const butil::StringPiece& name);
    void OnAudioMessage(brpc::RtmpAudioMessage* msg);
    void OnVideoMessage(brpc::RtmpVideoMessage* msg);
    int OnPause(bool pause_or_unpause, double /*offset_ms*/);

    // @RtmpStreamBase
    int SendMetaData(const brpc::RtmpMetaData& metadata, const butil::StringPiece& name);
    int SendVideoMessage(const brpc::RtmpVideoMessage& msg);
    int SendAudioMessage(const brpc::RtmpAudioMessage& msg);

    // @Describable
    void Describe(std::ostream& os, const brpc::DescribeOptions&) const;

    const std::string& key() const
    { return !_play_key.empty() ? _play_key : _publish_key; }

    const RtmpForwardServiceOptions& options() const
    { return _service->options(); }

    bool is_rtmp_user() const { return _is_rtmp_user; }

    RtmpForwardService* service() const { return _service; }

private:
friend class MonitoringServiceImpl;
friend class RtmpForwardService;

    void print_event_log(EventLogAction action) {
        _event_log_manager.print_event_log(
            action, _original_play_key, _sent_bytes, _received_bytes);
    }

    void reset_remote_side_in_event_log(butil::EndPoint end_point) {
        _event_log_manager.reset_remote_side(end_point);
    }

protected:
    RtmpForwardService* _service;
    PlayerGroupSharedPtr _players;
    PublishProxy* _publish_proxy;
    std::string _play_key;
    std::string _original_play_key;
    std::string _play_queries;
    std::string _publish_key;
    // [ tcurl = _tcurl_vhost/_tcurl_app/_stream_name_prefix ]
    // From RtmpConnectRequest.tcUrl, overridable by ?vhost/wshost/domain=
    // after stream_name.
    std::string _tcurl_vhost;
    // From RtmpConnectRequest.tcUrl, not overridable. No slashes inside.
    std::string _tcurl_app;
    std::string _stream_name_prefix;
    std::string _page_url;
    int64_t _play_or_publish_delay;
    int64_t _first_avc_seq_header_delay;
    int64_t _first_video_message_delay;
    int64_t _first_aac_seq_header_delay;
    int64_t _first_audio_message_delay;
    std::shared_ptr<brpc::RtmpMetaData> _metadata;
    double _expected_framerate;
    std::shared_ptr<brpc::RtmpVideoMessage> _avc_seq_header;
    std::shared_ptr<brpc::RtmpAudioMessage> _aac_seq_header;
    bool _audio_enabled;
    bool _video_enabled;
    bool _is_rtmp_user;
    size_t _sent_bytes;
    size_t _received_bytes;
    size_t _received_video_bytes;
    size_t _received_audio_bytes;
    size_t _received_video_frames;
    EventLogManager _event_log_manager;
    std::vector<butil::intrusive_ptr<PlayerInfo> > _reused_list;
    size_t _nrejected_videomsg;
    size_t _nrejected_audiomsg;
    int64_t _on_play_time_ms;
    std::string _tcurl;
    std::string _stream_name_with_query;
};

// Pull streams from another server and publish them locally.
class PlayerAsPublisher : public brpc::RtmpRetryingClientStream {
public:
    explicit PlayerAsPublisher(RtmpForwardService* service,
                               const std::string& key);
                               
    ~PlayerAsPublisher();

    // Must be called to initialize this object.
    int Init(brpc::SubStreamCreator* sub_stream_creator,
             // key is for computing hash code
             const std::string& key,
             // vhost/app are for unified play connections
             const butil::StringPiece& vhost,
             const butil::StringPiece& app,
             const butil::StringPiece& stream_name,
             const butil::StringPiece& queries,
             RtmpForwarder* stream);

    // @RtmpRetryingClientStream
    void OnUserData(void* user_data);
    void OnMetaData(brpc::RtmpMetaData* metadata, const butil::StringPiece& name);
    void OnAudioMessage(brpc::RtmpAudioMessage* msg);
    void OnVideoMessage(brpc::RtmpVideoMessage* msg);
    void OnStop();
    void OnPlayable();
    void OnFirstMessage();

private:

    RtmpForwardService* _service;
    butil::intrusive_ptr<RtmpForwarder> _local_publisher;
    std::string _key;
};

// [ Implementation ]

// ======== PlayerInfo ==========

PlayerInfo::PlayerInfo()
    : cursor(NULL)
    , sent_video_messages(0)
    , sent_audio_messages(0)
    , sent_user_messages(0)
    , stuck_video_times(0)
    , stuck_audio_times(0)
    , stuck_user_times(0)
    , sent_video_bytes(0)
    , sent_audio_bytes(0)
    , sent_video_frames(0)
    , keep_original_time(false)
    , is_outtest_cdn(false)
    , stuck_video_times_stored_by_dump_stats(0)
    , stuck_audio_times_stored_by_dump_stats(0)
    , _reset_base_timestamp(true)
    , _base_timestamp(0)
    , _last_timestamp(0) {
}

PlayerInfo::~PlayerInfo() {
    delete cursor;
    cursor = NULL;
}

void PlayerInfo::reset() {
    cursor->reset();
    sent_video_messages = 0;
    sent_audio_messages = 0;
    sent_user_messages = 0;
    stuck_video_times = 0;
    stuck_audio_times = 0;
    stuck_user_times = 0;
    sent_video_bytes = 0;
    sent_audio_bytes = 0;
    sent_video_frames = 0;
    keep_original_time = false;
    stuck_video_times_stored_by_dump_stats = 0;
    stuck_audio_times_stored_by_dump_stats = 0;
    sent_metadata.reset();
    sent_avc_seq_header.reset();
    sent_aac_seq_header.reset();
    _reset_base_timestamp = true;
    _base_timestamp = 0;
    // _last_timestamp must retain
}

void PlayerInfo::flush() {
    brpc::RtmpStreamBase* player = stream.get();
    Frame frame;
    while (cursor->next(&frame, keep_original_time)) {
        // Not all SendXXX impl. set errno on error, clear errno before
        // calling SendXXX so that errno must be explicitly set to enable
        // retrying at FAIL_TO_SEND.
        errno = 0;
        if (frame.metadata != NULL &&
            frame.metadata != sent_metadata) {
            brpc::RtmpMetaData cp = *frame.metadata;
            if (!keep_original_time) {
                if (_reset_base_timestamp) {
                    _reset_base_timestamp = false;
                    _base_timestamp = cp.timestamp - _last_timestamp;
                }
                cp.timestamp -= _base_timestamp;
                _last_timestamp = cp.timestamp;
            }
            if (player->SendMetaData(cp) != 0) {
                goto FAIL_TO_SEND;
            }
            sent_metadata = frame.metadata;
            VLOG(99) << player->remote_side() << ": Sent metadata="
                     << cp.data << " timestamp=" << cp.timestamp;
        }
        if (frame.avc_seq_header != NULL &&
            frame.avc_seq_header != sent_avc_seq_header) {
            brpc::RtmpVideoMessage avc_seq_header(*frame.avc_seq_header);
            if (keep_original_time) {
                // NOTE: KuaiShou requirement, the timestamp of seq header 
                // should be equal to the associated key frame. In the other 
                // case, this value is set to zero.
                avc_seq_header.timestamp = frame.timestamp;
            } else {
                avc_seq_header.timestamp += FLAGS_timestamp_offset;
            }
            if (player->SendVideoMessage(avc_seq_header) != 0) {
                goto FAIL_TO_SEND;
            }
            sent_avc_seq_header = frame.avc_seq_header;
            VLOG(99) << player->remote_side() << ": Sent avc_seq_header="
                     << *sent_avc_seq_header;
        }
        if (frame.aac_seq_header != NULL &&
            frame.aac_seq_header != sent_aac_seq_header) {
            brpc::RtmpAudioMessage aac_seq_header(*frame.aac_seq_header);
            if (keep_original_time) {
                // NOTE: KuaiShou requirement, the timestamp of seq header 
                // should be equal to the associated key frame. In the other 
                // case, this value is set to zero.
                aac_seq_header.timestamp = frame.timestamp;
            } else {
                aac_seq_header.timestamp += FLAGS_timestamp_offset;
            }
            if (player->SendAudioMessage(aac_seq_header) != 0) {
                goto FAIL_TO_SEND;
            }
            sent_aac_seq_header = frame.aac_seq_header;
            VLOG(99) << player->remote_side() << ": Sent aac_seq_header="
                     << *sent_aac_seq_header;
        }
        if (frame.type == FRAME_VIDEO) {
            brpc::RtmpVideoMessage msg;
            CHECK(frame.swap_as(msg));
            msg.timestamp += FLAGS_timestamp_offset;
            if (player->SendVideoMessage(msg) != 0) {
                ++stuck_video_times;
                goto FAIL_TO_SEND;
            }
            ++sent_video_messages;
            sent_video_bytes += msg.size();
            sent_video_frames += 1;
        } else if (frame.type == FRAME_AUDIO) {
            brpc::RtmpAudioMessage msg;
            CHECK(frame.swap_as(msg));
            msg.timestamp += FLAGS_timestamp_offset;
            if (player->SendAudioMessage(msg) != 0) {
                ++stuck_audio_times;
                goto FAIL_TO_SEND;
            }
            ++sent_audio_messages;
            sent_audio_bytes += msg.size();
        } else {
            LOG(ERROR) << "Unknown frame_type=" << frame.type;
        }
        continue;
        
    FAIL_TO_SEND:
        if (errno == brpc::ERTMPPUBLISHABLE) {
            reset();
            continue;
        } else {
            if (errno == EAGAIN ||
                errno == EPERM ||
                errno == brpc::EOVERCROWDED) {
                cursor->backup();
            }
            break;
        }
    }
}

// ========== PlayerGroup ==========

PlayerGroup::PlayerGroup(const std::string& key,
                         int64_t launcher_create_time,
                         int64_t first_play_or_publish_delay)
    : _puller(NULL)
    , _creating_puller(false)
    , frame_queue()
    , _start_idle_time_ms(0)
    , _has_removed(false)
    , _key(key)
    , _last_keyframe_index_in_video_frames(0)
    , _last_gop_size(0)
    , _video_bytes(0)
    , _video_frames(0)
    , _audio_bytes(0)
    , _audio_frames(0)
    , _stored_video_bytes(0)
    , _stored_video_frames(0)
    , _stored_audio_bytes(0)
    , _stored_audio_frames(0)
    , _video_bytes_last_second(0)
    , _audio_bytes_last_second(0)
    , _audio_frames_last_second(0)
    , _launcher_create_realtime_us(launcher_create_time)
    , _first_play_or_publish_delay(first_play_or_publish_delay)
    , _create_realtime_us(butil::gettimeofday_us())
    , _bvars(NULL) {
    frame_queue.set_name(key);
    pthread_mutex_init(&_member_mutex, NULL);
}

PlayerGroup::~PlayerGroup() {
    pthread_mutex_destroy(&_member_mutex);
    if (_puller) {
        _puller->Destroy();
        _puller = NULL;
    }
    delete _bvars;
    _bvars = NULL;
}

butil::EndPoint PlayerGroup::remote_side() const {
    butil::intrusive_ptr<PlayerAsPublisher> saved_puller(_puller);
    if (saved_puller != NULL) {
        // proxy mode, the publisher is local_publisher whose remote_side
        // is unset (all zero), use puller's.
        return saved_puller->remote_side();
    }
    butil::intrusive_ptr<RtmpForwarder> saved_publisher = _publisher;
    if (saved_publisher) {
        return saved_publisher->remote_side();
    }
    return butil::EndPoint();
}

void PlayerGroup::list_all(std::vector<butil::intrusive_ptr<PlayerInfo> >* list_out) {
    list_out->clear();
    std::unique_lock<pthread_mutex_t> mu(_member_mutex);
    list_out->insert(list_out->end(), _list.begin(), _list.end());
}

size_t PlayerGroup::count() {
    std::unique_lock<pthread_mutex_t> mu(_member_mutex);
    return _list.size();
}

bool PlayerGroup::queue(Frame& frame, const void* pusher) {
    // frame may be changed after push(), save the size for later usage.
    const size_t frame_size = frame.size();
    if (frame.type == FRAME_AUDIO) {
        if (!frame_queue.push(frame, pusher)) {
            return false;
        }
        _audio_bytes += frame_size;
        ++_audio_frames;
        if (FLAGS_show_streams_in_vars) {
            if (_bvars == NULL) {
                _bvars = new Bvars(_key);
            }
            _bvars->audio_bytes_total << frame_size;
        }
        return true;
    } else if (frame.type == FRAME_VIDEO) {
        const brpc::FlvVideoFrameType frame_type = frame.frame_type;
        if (!frame_queue.push(frame, pusher)) {
            return false;
        }
        if (frame_type == brpc::FLV_VIDEO_FRAME_KEYFRAME) {
            _last_gop_size = _video_frames - _last_keyframe_index_in_video_frames;
            _last_keyframe_index_in_video_frames = _video_frames;
        }
        _video_bytes += frame_size;
        ++_video_frames;
        if (FLAGS_show_streams_in_vars) {
            if (_bvars == NULL) {
                _bvars = new Bvars(_key);
            }
            _bvars->video_bytes_total << frame_size;
        }
        return true;
    } else if (frame.type == FRAME_USER_DATA) {
        if (!frame_queue.push(frame, pusher)) {
            LOG(ERROR) << "Fail to push to frame_queue";
            return false;
        }
    } else {
        LOG(FATAL) << "Unknown frame_type=" << frame.type;
        return false;
    }
    return true;
}

void PlayerGroup::update_stats_every_second() {
    const size_t saved_video_bytes = _video_bytes;
    const size_t saved_video_frames = _video_frames;
    const size_t saved_audio_bytes = _audio_bytes;
    const size_t saved_audio_frames = _audio_frames;
    _audio_bytes_last_second = saved_audio_bytes - _stored_audio_bytes;
    _audio_frames_last_second = saved_audio_frames - _stored_audio_frames;
    _video_bytes_last_second = saved_video_bytes - _stored_video_bytes;
    _video_frames_second.add(saved_video_frames - _stored_video_frames);
    _stored_audio_bytes = saved_audio_bytes;
    _stored_audio_frames = saved_audio_frames;
    _stored_video_bytes = saved_video_bytes;
    _stored_video_frames = saved_video_frames;
}

// ========== RtmpForwarder ==========

// True if `s' only contains digits and dots. Strictly this function filters
// more than an IP, but should be OK in most cases.
inline bool only_digits_and_dots(const butil::StringPiece& s) {
    for (size_t i = 0; i < s.size(); ++i) {
        if (!::isdigit(s[i]) && s[i] != '.') {
            return false;
        }
    }
    return true;
}

bool is_user_defined_vhost(const butil::StringPiece& vhost) {
    return !vhost.empty() && vhost != FLAGS_default_vhost;
}

std::string get_vhost(const butil::StringPiece& host,
                      const butil::StringPiece& vhost) {
    butil::StringPiece vhost_candidate = (vhost.empty() ? host : vhost);

    // Rewrite special vhosts to default vhost.
    butil::StringPiece myhost = butil::my_hostname();
    if (vhost_candidate == myhost ||
        vhost_candidate == "localhost" ||
        vhost_candidate == SRS_DEFAULT_VHOST ||
        only_digits_and_dots(vhost_candidate)) {
        vhost_candidate = FLAGS_default_vhost;
    }

    // if vhost ends with legacy vhost, make it as default vhost
    if (vhost_candidate == FLAGS_default_vhost) {
        // Don't turn default vhost lowercased
        return FLAGS_default_vhost;
    }
    std::string vhost_result;
    vhost_candidate.CopyToString(&vhost_result);
    // following nginx, lowercase host.
    to_lowercase(&vhost_result);
    return vhost_result;
}

RtmpForwarder::RtmpForwarder(RtmpForwardService* service,
                             const brpc::RtmpConnectRequest& conn_req,
                             bool is_rtmp_user)
    : _service(service)
    , _publish_proxy(NULL)
    , _play_or_publish_delay(0)
    , _first_avc_seq_header_delay(0)
    , _first_video_message_delay(0)
    , _first_aac_seq_header_delay(0)
    , _first_audio_message_delay(0)
    , _expected_framerate(0)
    , _audio_enabled(true)
    , _video_enabled(true)
    , _is_rtmp_user(is_rtmp_user)
    , _sent_bytes(0)
    , _received_bytes(0)
    , _received_video_bytes(0)
    , _received_audio_bytes(0)
    , _received_video_frames(0)
    , _event_log_manager(EVENT_LOG_RTMP, 
                         service->options().internal_service, 
                         butil::EndPoint() /* this value will be reset in OnPlay*/)
    , _nrejected_videomsg(0)
    , _nrejected_audiomsg(0)
    , _on_play_time_ms(0)
    , _tcurl(conn_req.tcurl()) {
    if (!_service->options().internal_service && _is_rtmp_user) {
        g_rtmp_user_count << 1;
    }
    butil::StringPiece host;
    butil::StringPiece vhost;
    butil::StringPiece app;
    butil::StringPiece stream_name_prefix;
    brpc::ParseRtmpURL(conn_req.tcurl(), &host, &vhost, NULL, &app,
                             &stream_name_prefix);
    _tcurl_vhost = get_vhost(host, vhost);
    app.CopyToString(&_tcurl_app);
    stream_name_prefix.CopyToString(&_stream_name_prefix);
    _page_url = conn_req.pageurl();
}

RtmpForwarder::~RtmpForwarder() {
    if (!_service->options().internal_service && _is_rtmp_user) {
        g_rtmp_user_count << -1;
    }
    VLOG(99) << __FUNCTION__ << '(' << this << "), "
             << (!_play_key.empty() ? "play" : "publish") << "_key="
             << (!_play_key.empty() ?  _play_key : _publish_key)
             << " remote_side=" << remote_side();
}

void RtmpForwarder::Describe(std::ostream& os,
                             const brpc::DescribeOptions&) const {
    os << brpc::PrintedAsDateTime(create_realtime_us())
       << " play_delay=" << _play_or_publish_delay / 1000.0
       << "ms vsh1=" << _first_avc_seq_header_delay / 1000.0
       << "ms v1=" << _first_video_message_delay / 1000.0
       << "ms ash1=" << _first_aac_seq_header_delay / 1000.0
       << "ms a1=" << _first_audio_message_delay / 1000.0
       << "ms";
}

void RtmpForwarder::OnStop() {
    VLOG(99) << "RtmpForwarder::OnStop(" << this << "), "
             << (!_play_key.empty() ? "play" : "publish") << "_key="
             << (!_play_key.empty() ?  _play_key : _publish_key);
    if (!_play_key.empty()) {
        _service->remove_player(_play_key, this);
        print_event_log(EVENT_LOG_STOP_PLAY);
    }
    if (!_publish_key.empty()) {
        _service->remove_publisher(_publish_key, this);
        if (_publish_proxy) {
            _service->remove_player(_publish_key, _publish_proxy);
        }
    }
    if (_publish_proxy) {
        _publish_proxy->Destroy();
        _publish_proxy = NULL;
    }
}

void remove_audio_video_selections(std::string* queries,
                                   bool* audio_only,
                                   bool* video_only) {
    bool removed_sth = false;
    for (brpc::QuerySplitter qs(*queries); qs; ++qs) {
        butil::StringPiece q = qs.key_and_value();
        if (q == MEDIA_SERVER_AUDIO_ONLY_TAG "=1" ||
            q == MEDIA_SERVER_VIDEO_ONLY_TAG "=1") {
            removed_sth = true;
            break;
        }
    }
    if (!removed_sth) {
        return;
    }
    
    brpc::QueryRemover qr(queries);
    for (; qr; ++qr) {
        butil::StringPiece q = qr.key_and_value();
        if (q == MEDIA_SERVER_AUDIO_ONLY_TAG "=1") {
            if (audio_only) {
                *audio_only = true;
            }
            qr.remove_current_key_and_value();
        } else if (q == MEDIA_SERVER_VIDEO_ONLY_TAG "=1") {
            if (video_only) {
                *video_only = true;
            }
            qr.remove_current_key_and_value();
        }
    }

    *queries = qr.modified_query();
}

void remove_audio_releated_fields(brpc::AMFObject* metadata) {
    metadata->Remove("audiochannels");
    metadata->Remove("audiosamplerate");
    metadata->Remove("audiosamplesize");
    metadata->Remove("audiodatarate");
    metadata->Remove("audiocodecid");
    metadata->Remove("stereo");
}

void remove_video_releated_fields(brpc::AMFObject* metadata) {
    metadata->Remove("width");
    metadata->Remove("height");
    metadata->Remove("framerate");
    metadata->Remove("videodatarate");
    metadata->Remove("videocodecid");
}

// Remove vhost=X/wshost=X/domain=X and app=Y from *queries.
// the overwriting order is vhost > wshost > domain
static void remove_vhost_and_app_from_queries(std::string* queries/*inout*/,
                                              std::string* vhost_out,
                                              std::string* app_out) {
    butil::StringPiece vhost;
    butil::StringPiece wshost;
    butil::StringPiece domain;
    butil::StringPiece app;
    std::string new_queries;
    if (!queries) {
        return;
    }

    brpc::QueryRemover qr(queries);
    for (; qr; ++qr) {
        butil::StringPiece name = qr.key();
        butil::StringPiece value = qr.value();
        if (name == "vhost") {
            vhost = value;
            qr.remove_current_key_and_value();
        } else if (name == "wshost" || name == "wsHost") {
            wshost = value;
            qr.remove_current_key_and_value();
        } else if (name == "domain") {
            domain = value;
            qr.remove_current_key_and_value();
        } else if (name == "app") {
            app = value;
            qr.remove_current_key_and_value();
        }
    }

    if (vhost.empty()) {
        vhost = (!wshost.empty() ? wshost : domain);
    }
    // Notice that setting vhost_out/app_out should be done before overwriting
    // *queries which is referenced by vhost/app (StringPiece)
    if (vhost_out) {
        vhost_out->assign(vhost.data(), vhost.size());
    }
    if (app_out) {
        app_out->assign(app.data(), app.size());
    }

    *queries = qr.modified_query();
}

bool keep_original_timestamp(const butil::StringPiece& vhost) {
    butil::StringSplitter sp(FLAGS_original_timestamp_domains.c_str(), ' ');
    for (; sp; ++sp) {
        butil::StringPiece host(sp.field(), sp.length());
        if (host == vhost) {
            return true;
        }
    }
    return false;
}

bool keep_duration_in_metadata(const butil::StringPiece& vhost) {
    butil::StringSplitter sp(FLAGS_keep_duration_domains.c_str(), ' ');
    for (; sp; ++sp) {
        butil::StringPiece host(sp.field(), sp.length());
        if (host == vhost) {
            return true;
        }
    }
    return false;
}

bool is_keep_time() {
    // Implement your keep time policy
    return false;
}

void RtmpForwarder::OnPlay(const brpc::RtmpPlayOptions& opt,
                           butil::Status* status,
                           google::protobuf::Closure* play_done) {
    brpc::ClosureGuard play_done_guard(play_done);
    if (!_play_or_publish_delay) {
        _play_or_publish_delay = butil::gettimeofday_us() - create_realtime_us();
    }
    // Append non-empty _stream_name_prefix to playing name.
    butil::StringPiece queries;
    butil::StringPiece stream_name_from_play = brpc::RemoveQueryStrings(
        opt.stream_name, &queries);
    butil::StringPiece stream_name;
    std::string stream_name_str;
    if (_stream_name_prefix.empty()) {
        stream_name = stream_name_from_play;
    } else {
        stream_name_str.reserve(stream_name_from_play.size() + 1 +
                                _stream_name_prefix.size());
        stream_name_str.append(_stream_name_prefix);
        stream_name_str.push_back('/');
        stream_name_str.append(stream_name_from_play.data(),
                               stream_name_from_play.size());
        stream_name = stream_name_str;
    }
    
    _play_queries = queries.as_string();

    std::string vhost;
    std::string app;
    remove_vhost_and_app_from_queries(&_play_queries, &vhost, &app);
    if (vhost.empty()) {
        vhost = _tcurl_vhost;
    }
    if (app.empty()) {
        app = _tcurl_app;
    }
    std::string new_original_play_key = make_key(vhost, app, stream_name);
    std::string new_play_key = new_original_play_key;
    if (options().auth_play == "config") {
        // Do the authentication by user configuration.
        // It should be implemented by service provider.
    }
    if (!_play_queries.empty()) {
        bool audio_only = false;
        bool video_only = false;
        // Remove audio-only/video-only from queries so that pulled streams
        // are not affected.
        remove_audio_video_selections(&_play_queries, &audio_only, &video_only);
        if (audio_only && video_only) {
            LOG(WARNING) << MEDIA_SERVER_AUDIO_ONLY_TAG " and "
                MEDIA_SERVER_VIDEO_ONLY_TAG " are not allowed to be present "
                "at same time, ignore both";
        } else {
            _video_enabled = !audio_only;
            _audio_enabled = !video_only;
        }
    }

    bool keep_time = is_keep_time();

    if (new_play_key == _play_key) {
        if (new_original_play_key != _original_play_key) {
            // When it comes to same play key(which means same player group) but different original key, 
            // we need to stop the previous log, and update the _original_play_key
            print_event_log(EVENT_LOG_STOP_PLAY);
            _original_play_key = new_original_play_key;
        }
        return;
    }
    if (!_play_key.empty()) {
        // Unregister from previous playing stream.
        _service->remove_player(_play_key, this);
        print_event_log(EVENT_LOG_STOP_PLAY);
        _play_key.clear();
    }
    // we need to set remote_side here instead of in the constructor(e.g., FLV and HLS)
    // is because the value of remote_side is not ready when this RTMPForwarder was created.
    reset_remote_side_in_event_log(remote_side());

    // Updating _play_key before add_player() is a must because the function
    // may send existing data to the player directly, where may need _play_key
    _play_key = new_play_key;
    _original_play_key = new_original_play_key;
    _on_play_time_ms = butil::gettimeofday_ms();
    
    *status = _service->add_player(_play_key, _play_queries,
                                   this, _play_or_publish_delay, keep_time);
}

int RtmpForwarder::SendMetaData(const brpc::RtmpMetaData& metadata, const butil::StringPiece& name) {
    if (_audio_enabled && _video_enabled) {
        return RtmpStreamBase::SendMetaData(metadata, name);
    }
    brpc::RtmpMetaData cp = metadata;
    if (!_audio_enabled) {
        remove_audio_releated_fields(&cp.data);
    }
    if (!_video_enabled) {
        remove_video_releated_fields(&cp.data);
    }
    return RtmpStreamBase::SendMetaData(cp, name);
}

int RtmpForwarder::SendVideoMessage(const brpc::RtmpVideoMessage& msg) {
    if (!_video_enabled) {
        return 0;
    }
    _sent_bytes += msg.size();
    print_event_log(EVENT_LOG_START_PLAY);

    if (msg.IsAVCSequenceHeader() || msg.IsHEVCSequenceHeader()) {
        if (!_first_avc_seq_header_delay) {
            _first_avc_seq_header_delay = butil::gettimeofday_us()
                - create_realtime_us() - _play_or_publish_delay;
        }
    } else {
        if (!_first_video_message_delay) {
            _first_video_message_delay = butil::gettimeofday_us()
                - create_realtime_us() - _play_or_publish_delay;
        }
    }
    return RtmpStreamBase::SendVideoMessage(msg);
}

int RtmpForwarder::SendAudioMessage(const brpc::RtmpAudioMessage& msg) {
    if (!_audio_enabled) {
        return 0;
    }
    _sent_bytes += msg.size();
    print_event_log(EVENT_LOG_START_PLAY);
        
    if (msg.IsAACSequenceHeader()) {
        if (!_first_aac_seq_header_delay) {
            _first_aac_seq_header_delay = butil::gettimeofday_us()
                - create_realtime_us() - _play_or_publish_delay;
        }
    } else {
        if (!_first_audio_message_delay) {
            _first_audio_message_delay = butil::gettimeofday_us()
                - create_realtime_us() - _play_or_publish_delay;
        }
    }
    return RtmpStreamBase::SendAudioMessage(msg);
}

class PublishingClientSelector : public RtmpClientSelector {
public:
    PublishingClientSelector(const std::string& key,
                             RtmpForwardService* service)
        : _key(key)
        , _service(service) {}
    // @RtmpClientSelector
    void StartGettingRtmpClient(OnGetRtmpClient* done);
private:
    const std::string _key;
    RtmpForwardService* const _service;
};

void PublishingClientSelector::StartGettingRtmpClient(
    OnGetRtmpClient* done) {
    butil::StringPiece vhost;
    butil::StringPiece app;
    butil::StringPiece stream_name;
    brpc::ParseRtmpURL(_key, &vhost, NULL, NULL, &app, &stream_name);
    _service->get_proxy_client(vhost, app, stream_name, butil::StringPiece(), true,
                               false/* In publish, just use regular rtmp protocol */, 
                               done);
}

// Get a RtmpClient to the target server.
class GetOrNewRtmpClient : public OnGetTargetServer {
public:
    GetOrNewRtmpClient()
        : done(NULL)
        , is_publish(false)
        , simplified_rtmp(false) {}
    void on_get_target_server(const butil::Status& status,
                              const char* server_addr,
                              const char* lb_algo);
public:
    std::string vhost;
    std::string app;
    std::string stream;
    OnGetRtmpClient* done;
    bool is_publish;
    bool simplified_rtmp;
};

void GetOrNewRtmpClient::on_get_target_server(
    const butil::Status& status,
    const char* server_addr,
    const char* lb_algo) {
    std::unique_ptr<GetOrNewRtmpClient> delete_self(this);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to get target server: " << status;
        return done->Run(NULL);
    }
    std::string key = make_key(vhost, app, stream);
    done->server_addr = server_addr;
    done->key = key;
    std::string tcurl;
    std::string clientkey;
    if (!is_publish && FLAGS_unified_play_connections) {
        tcurl = RTMP_UNIFIED_TCURL;
        const size_t addr_len = strlen(server_addr);
        clientkey.reserve(addr_len + 1 + tcurl.size());
        clientkey.append(server_addr, addr_len);
        clientkey.push_back('/');
        clientkey.append(tcurl);
    } else if (is_user_defined_vhost(vhost)) {
        tcurl = brpc::MakeRtmpURL(vhost, "", app, "");
        const size_t addr_len = strlen(server_addr);
        clientkey.reserve(addr_len + 1 + tcurl.size());
        clientkey.append(server_addr, addr_len);
        clientkey.push_back('/');
        clientkey.append(tcurl);
    } else {
        clientkey = brpc::MakeRtmpURL(server_addr, "", app, "");
        if (butil::StringPiece(server_addr).find("://")
            == butil::StringPiece::npos) {
            // server_addr is likely to be hostname or ip.
            tcurl = clientkey;
        } else {
            // server_addr is probably a naming service, just leave
            // it empty to be filled in rtmp_protocol.cpp
        }
    }
    std::unique_lock<pthread_mutex_t> mu(s_proxy_client_map_mutex);
    if (s_proxy_client_map == NULL) {
        s_proxy_client_map = new (std::nothrow) ProxyClientMap;
        if (s_proxy_client_map == NULL) {
            mu.unlock();
            LOG(WARNING) << "Fail to new s_proxy_client_map";
            return done->Run(NULL);
        }
    }

    brpc::RtmpClient* client = &(*s_proxy_client_map)[clientkey];
    if (client->initialized()) {
        mu.unlock();
        return done->Run(client);
    }
    mu.unlock();
    brpc::RtmpClient tmp_client;
    brpc::RtmpClientOptions rtmp_opt;
    rtmp_opt.app = app;
    rtmp_opt.tcUrl = tcurl;
    rtmp_opt.simplified_rtmp = simplified_rtmp;
    rtmp_opt.connect_timeout_ms =
        (FLAGS_connect_timeout_ms > 0 ?
         FLAGS_connect_timeout_ms : FLAGS_timeout_ms / 3);
    rtmp_opt.timeout_ms = FLAGS_timeout_ms;
    VLOG(99) << "Creating proxy client to " << server_addr << ", lb_algo="
             << lb_algo << " tcUrl=" << rtmp_opt.tcUrl
             << " clientkey=" << clientkey;
    if (tmp_client.Init(server_addr, lb_algo, rtmp_opt) != 0) {
        LOG(ERROR) << "Fail to create proxy client to " << server_addr
                   << ", lb_algo=" << lb_algo << " tcUrl=" << rtmp_opt.tcUrl
                   << " clientkey=" << clientkey;
        return done->Run(NULL);
    }
    {
        std::unique_lock<pthread_mutex_t> mu2(s_proxy_client_map_mutex);
        if (!client->initialized()) {
            *client = tmp_client;
        }
    }
    return done->Run(client);
}

PublishProxy::PublishProxy(const std::string& key, RtmpForwarder* publisher)
    : _key(key)
    , _publisher(publisher) {
}

PublishProxy::~PublishProxy() {}

int PublishProxy::SendVideoMessage(const brpc::RtmpVideoMessage& msg) {
    // You can call StopCurrentStream() to re-choose idc to play
    return RtmpRetryingClientStream::SendVideoMessage(msg);
}

int PublishProxy::SendAudioMessage(const brpc::RtmpAudioMessage& msg) {
    return RtmpRetryingClientStream::SendAudioMessage(msg);
}

int PublishProxy::SendMetaData(const brpc::RtmpMetaData& metadata, const butil::StringPiece& name) {
    return RtmpRetryingClientStream::SendMetaData(metadata, name);
}

void PublishProxy::OnStop() {
    std::string err = butil::string_printf(
        "PublishProxy on %s to %s was stopped",
        _key.c_str(), butil::endpoint2str(_publisher->remote_side()).c_str());
    _publisher->SendStopMessage(err);
    _publisher = NULL;
}

void RtmpForwarder::OnPublish(const std::string& stream_name_in,
                              brpc::RtmpPublishType type,
                              butil::Status* status,
                              google::protobuf::Closure* publish_done) {
    brpc::ClosureGuard publish_done_guard(publish_done);
    if (!_play_or_publish_delay) {
        _play_or_publish_delay = butil::gettimeofday_us() - create_realtime_us();
    }

    // Append non-empty _stream_name_prefix to publishing name
    butil::StringPiece queries_piece;
    butil::StringPiece stream_name_from_pub = brpc::RemoveQueryStrings(
        stream_name_in, &queries_piece);
    butil::StringPiece stream_name;
    std::string stream_name_str;
    if (_stream_name_prefix.empty()) {
        stream_name = stream_name_from_pub;
    } else {
        stream_name_str.reserve(stream_name_from_pub.size() + 1 +
                                _stream_name_prefix.size());
        stream_name_str.append(_stream_name_prefix);
        stream_name_str.push_back('/');
        stream_name_str.append(stream_name_from_pub.data(),
                               stream_name_from_pub.size());
        stream_name = stream_name_str;
    }
    std::string queries(queries_piece.data(), queries_piece.size());
    std::string vhost;
    std::string app;
    remove_vhost_and_app_from_queries(&queries, &vhost, &app);
    if (vhost.empty()) {
        vhost = _tcurl_vhost;
    }
    if (app.empty()) {
        app = _tcurl_app;
    }
    std::string new_publish_key = make_key(vhost, app, stream_name);
    bool keep_time = is_keep_time();

    if (options().proxy_to.empty() || type == RTMP_PUBLISH_FAKE) {
        _publish_key = new_publish_key;
        if (_service->add_publisher(new_publish_key, this, &_players,
                                    _play_or_publish_delay) != 0) {
            status->set_error(EPERM, "Fail to publish %s", new_publish_key.c_str());
            return;
        }
    } else {
        if (_publish_proxy != NULL) {
            status->set_error(EPERM, "%s already has publish_proxy",
                              new_publish_key.c_str());
            return;
        }
        
        _publish_key = new_publish_key;
        _publish_key.append(PUBLISH_PROXY_SUFFIX);
        if (_service->add_publisher(_publish_key, this, &_players,
                                    _play_or_publish_delay) != 0) {
            status->set_error(EPERM, "Fail to publish %s", new_publish_key.c_str());
            return;
        }

        _publish_proxy = new PublishProxy(new_publish_key, this);
        brpc::RtmpRetryingClientStreamOptions opt;
        opt.retry_interval_ms = FLAGS_retry_interval_ms;
        // TODO: max_retry_duration_ms may not work for publish.
        opt.max_retry_duration_ms = FLAGS_max_retry_duration_ms;
        opt.fast_retry_count = FLAGS_fast_retry_count;
        opt.share_connection = options().share_publish_connection;
        opt.quit_when_no_data_ever = true;
        // Always set request_code to make consistent hashing work.
        opt.hash_code = brpc::policy::MurmurHash32(
            new_publish_key.data(), new_publish_key.size());
        std::string new_queries(queries.data(), queries.size());

        opt.publish_name.reserve(stream_name.size() + 1 + new_queries.size());
        opt.publish_name.append(stream_name.data(), stream_name.size());
        if (!new_queries.empty()) {
            opt.publish_name.push_back('?');
            opt.publish_name.append(new_queries);
        }
        opt.publish_type = type;

        PublishingClientSelector* client_selector =
            new PublishingClientSelector(new_publish_key, _service);
        _publish_proxy->Init(new RtmpSubStreamCreator(client_selector, false), opt);
        *status = _service->add_player(_publish_key, "", _publish_proxy, 0, keep_time);
    }
}

void RtmpForwarder::OnUserData(void* user_message) {
    return;
}

void RtmpForwarder::OnMetaData(brpc::RtmpMetaData* metadata, const butil::StringPiece& name) {
    if (_players == NULL) {
        LOG(WARNING) << remote_side() << '[' << stream_id()
                     << "] Rejected metadata";
        return;
    }
    _metadata.reset(new brpc::RtmpMetaData(*metadata));
    // cyberplayer treats the stream as recorded when `duration' is
    // present and sends a seek to the server and makes the player
    // buffering(black-screen). Remove the field to prevent this
    // from happening.
    // But some domains like Kuaishou need to keep 'duration'
    butil::StringPiece vhost;
    brpc::ParseRtmpURL(_publish_key, &vhost, NULL, NULL, NULL, NULL);
    if (!keep_duration_in_metadata(vhost)) {
        _metadata->data.Remove("duration");
    }

    const brpc::AMFField* fr = _metadata->data.Find("framerate");
    if (fr) {
        if (fr->IsNumber()) {
            _expected_framerate = fr->AsNumber();
        }
    }
}

void RtmpForwarder::OnAudioMessage(brpc::RtmpAudioMessage* msg) {
    _received_bytes += msg->size();
    _received_audio_bytes += msg->size();

    if (msg->IsAACSequenceHeader()) {
        if (!_first_aac_seq_header_delay) {
            _first_aac_seq_header_delay = butil::gettimeofday_us() - create_realtime_us();
        }
        _aac_seq_header.reset(new brpc::RtmpAudioMessage(*msg));
        _aac_seq_header->timestamp = 0;
        return;
    }
    if (!_first_audio_message_delay) {
        _first_audio_message_delay = butil::gettimeofday_us() - create_realtime_us();
    }    
    Frame frame(*msg);
    frame.metadata = _metadata;
    frame.avc_seq_header = _avc_seq_header;
    frame.aac_seq_header = _aac_seq_header;
    if (_players == NULL || !_players->queue(frame, this)) {
        ++_nrejected_audiomsg;
        LOG_EVERY_SECOND(WARNING)
            << remote_side() << '[' << stream_id()
            << "] Rejected " << _nrejected_audiomsg << " AudioMessages";
        return;
    }
    _players->list_all(&_reused_list);
    while (!_reused_list.empty()) {
        _reused_list.back()->flush();
        _reused_list.pop_back();
    }
}

void RtmpForwarder::OnVideoMessage(brpc::RtmpVideoMessage* msg) {
    _received_bytes += msg->size();
    _received_video_bytes += msg->size();
    _received_video_frames += 1;
    if (msg->IsAVCSequenceHeader() || msg->IsHEVCSequenceHeader()) {
        if (!_first_avc_seq_header_delay) {
            _first_avc_seq_header_delay = butil::gettimeofday_us() - create_realtime_us();
        }
        _avc_seq_header.reset(new brpc::RtmpVideoMessage(*msg));
        _avc_seq_header->timestamp = 0;
        return;
    }
    if (!_first_video_message_delay) {
        _first_video_message_delay = butil::gettimeofday_us() - create_realtime_us();
    }
    Frame frame(*msg);
    frame.metadata = _metadata;
    frame.avc_seq_header = _avc_seq_header;
    frame.aac_seq_header = _aac_seq_header;
    if (_players == NULL || !_players->queue(frame, this)) {
        ++_nrejected_videomsg;
        LOG_EVERY_SECOND(WARNING)
            << remote_side() << '[' << stream_id()
            << "] Rejected " << _nrejected_videomsg << " VideoMessages";
        return;
    }
    _players->list_all(&_reused_list);
    while (!_reused_list.empty()) {
        _reused_list.back()->flush();
        _reused_list.pop_back();
    }
}

static void key_to_url(const butil::StringPiece& key,
                       int port,
                       std::string* rtmpurl,
                       std::string* flvurl,
                       std::string* hlsurl) {
    butil::StringPiece vhost;
    butil::StringPiece app;
    butil::StringPiece stream_name;
    brpc::ParseRtmpURL(key, &vhost, NULL, NULL, &app, &stream_name);
    butil::StringPiece host(butil::my_hostname());
    const bool has_vhost = is_user_defined_vhost(vhost);
    if (rtmpurl) {
        rtmpurl->reserve(7/*prefix*/ + host.size() + 6/*port*/ + 1/*slash*/
                         + app.size() + (has_vhost ? (vhost.size() + 7) : 0)
                         + 1/*slash*/ + stream_name.size());
        rtmpurl->append("rtmp://");
        rtmpurl->append(host.data(), host.size());
        butil::string_appendf(rtmpurl, ":%d", port);
        rtmpurl->push_back('/');
        rtmpurl->append(app.data(), app.size());
        if (has_vhost) {
            rtmpurl->append("?vhost=");
            rtmpurl->append(vhost.data(), vhost.size());
        }
        rtmpurl->push_back('/');
        rtmpurl->append(stream_name.data(), stream_name.size());
    }
    
    if (flvurl) {
        flvurl->reserve(7/*prefix*/ + host.size() + 6/*port*/ + 1/*slash*/
                        + app.size() + 1/*slash*/ + stream_name.size()
                        + 4/*suffix*/ + (has_vhost ? (vhost.size() + 9) : 0));
        flvurl->append("http://");
        flvurl->append(host.data(), host.size());
        butil::string_appendf(flvurl, ":%d", port);
        flvurl->push_back('/');
        flvurl->append(app.data(), app.size());
        flvurl->push_back('/');
        flvurl->append(stream_name.data(), stream_name.size());
        flvurl->append(".flv");
        if (has_vhost) {
            flvurl->append("%3Fvhost=");
            flvurl->append(vhost.data(), vhost.size());
        }
    }

    if (hlsurl) {
        hlsurl->reserve(7/*prefix*/ + host.size() + 6/*port*/ + 1/*slash*/
                        + app.size() + 1/*slash*/ + stream_name.size()
                        + 5/*suffix*/ + (has_vhost ? (vhost.size() + 9) : 0));
        hlsurl->append("http://");
        hlsurl->append(host.data(), host.size());
        butil::string_appendf(hlsurl, ":%d", port);
        hlsurl->push_back('/');
        hlsurl->append(app.data(), app.size());
        hlsurl->push_back('/');
        hlsurl->append(stream_name.data(), stream_name.size());
        hlsurl->append(".m3u8");
        if (has_vhost) {
            hlsurl->append("%3Fvhost=");
            hlsurl->append(vhost.data(), vhost.size());
        }
    }
}

int RtmpForwarder::OnPause(bool pause_or_unpause, double /*offset_ms*/) {
    if (_play_key.empty()) {
        return -1;
    }
    if (pause_or_unpause) {
        _service->remove_player(_play_key, this);
        print_event_log(EVENT_LOG_STOP_PLAY);
        return 0;
    } else {
        butil::StringPiece vhost;
        brpc::ParseRtmpURL(_play_key, &vhost, NULL, NULL, NULL, NULL);
        butil::Status st = _service->add_player(_play_key, _play_queries, this, 0, keep_original_timestamp(vhost));
        if (!st.ok()) {
            SendStopMessage(st.error_cstr());
            LOG(WARNING) << "Fail to add_player: " << st;
            return -1;
        }
        return 0;
    }
}

// ======== PlayerAsPublisher ==========

PlayerAsPublisher::PlayerAsPublisher(RtmpForwardService* service,
                                     const std::string& key)
    : _service(service)
    , _key(key) {
}

PlayerAsPublisher::~PlayerAsPublisher() {
    VLOG(99) << __FUNCTION__ << '(' << this << "), local_publisher="
             << _local_publisher.get();
}

int PlayerAsPublisher::Init(brpc::SubStreamCreator* sub_stream_creator,
                            const std::string& key,
                            const butil::StringPiece& vhost, 
                            const butil::StringPiece& app,
                            const butil::StringPiece& stream_name,
                            const butil::StringPiece& queries,
                            RtmpForwarder* local_publisher) {
    std::unique_ptr<brpc::SubStreamCreator> sub_stream_creator_guard(sub_stream_creator);
    _local_publisher.reset(local_publisher);
    butil::Status status;
    std::string stream_name_str;
    stream_name.CopyToString(&stream_name_str);
    _local_publisher->OnPublish(stream_name_str, RTMP_PUBLISH_FAKE,
                                &status, brpc::DoNothing());
    if (!status.ok()) {
        LOG(ERROR) << "Fail to publish stream=" << stream_name << " locally";
        return -1;
    }
    brpc::RtmpRetryingClientStreamOptions opt;
    opt.retry_interval_ms = FLAGS_retry_interval_ms;
    opt.max_retry_duration_ms = FLAGS_max_retry_duration_ms;
    opt.fast_retry_count = FLAGS_fast_retry_count;
    opt.share_connection = _service->options().share_play_connection;
    opt.quit_when_no_data_ever = true;
    // Always set request_code to make consistent hashing work.
    opt.hash_code = brpc::policy::MurmurHash32(key.data(), key.size());
    
    if (FLAGS_unified_play_connections) {
        opt.play_name.reserve(stream_name_str.size() + 1/*?*/ + queries.size() +
                              7/*&vhost=*/ + vhost.size() +
                              5/*&app=*/ + app.size());
        opt.play_name.append(stream_name_str);
        opt.play_name.push_back('?');
        if (!queries.empty()) {
            opt.play_name.append(queries.data(), queries.size());
        }
        if (is_user_defined_vhost(vhost)) {
            brpc::append_query(&opt.play_name, "vhost", vhost);
        }
        brpc::append_query(&opt.play_name, "app", app);
    } else if (queries.empty()) {
        opt.play_name = stream_name_str;
    } else {
        opt.play_name.reserve(stream_name_str.size() + 1 + queries.size());
        opt.play_name.append(stream_name_str);
        opt.play_name.push_back('?');
        opt.play_name.append(queries.data(), queries.size());
    }
    RtmpRetryingClientStream::Init(sub_stream_creator_guard.release(), opt);
    return 0;
}

void PlayerAsPublisher::OnUserData(void* user_message) {
    _local_publisher->OnUserData(user_message);
}

void PlayerAsPublisher::OnMetaData(brpc::RtmpMetaData* metadata, const butil::StringPiece& name) {
    _local_publisher->OnMetaData(metadata, name);
}

void PlayerAsPublisher::OnAudioMessage(brpc::RtmpAudioMessage* msg) {
    _local_publisher->OnAudioMessage(msg);
}

void PlayerAsPublisher::OnVideoMessage(brpc::RtmpVideoMessage* msg) {
    // You can call StopCurrentStream() to re-choose idc to play
    _local_publisher->OnVideoMessage(msg);
}

void PlayerAsPublisher::OnPlayable() {
    std::string key = _local_publisher->key();
    if (_service->reset_publisher(key, _local_publisher.get()) != 0) {
        LOG(ERROR) << "Fail to reset local_publisher=" << _local_publisher.get()
                   << " of puller=" << this << " of " << key;
        return;
    } 
    VLOG(99) << "Reset local_publisher=" << _local_publisher.get()
             << " of puller=" << this << " of " << key;
}

void PlayerAsPublisher::OnFirstMessage() { }

void PlayerAsPublisher::OnStop() {
    if (_local_publisher) {
        _local_publisher->OnStop();
    }
}

// ======== RtmpForwardService ==========

void* RtmpForwardService::run_print_stats(void* arg) {
    static_cast<RtmpForwardService*>(arg)->print_stats();
    return NULL;
}

void* RtmpForwardService::run_update_stats(void* arg) {
    static_cast<RtmpForwardService*>(arg)->update_stats();
    return NULL;
}

void RtmpForwardService::update_stats() {
    const int64_t start_time_us = butil::gettimeofday_us();
    const int WARN_NOSLEEP_THRESHOLD = 2;
    int64_t last_time_us = start_time_us;
    int consecutive_nosleep = 0;
    std::vector<std::pair<std::string, PlayerGroupSharedPtr> > list;
    while (!bthread_stopped(bthread_self())) {
        const int64_t sleep_us = 1000000L + last_time_us - butil::gettimeofday_us();
        if (sleep_us > 0) {
            if (bthread_usleep(sleep_us) < 0) {
                PLOG_IF(FATAL, errno != ESTOP) << "Fail to sleep";
                break;
            }
            consecutive_nosleep = 0;
        } else {            
            if (++consecutive_nosleep >= WARN_NOSLEEP_THRESHOLD) {
                consecutive_nosleep = 0;
                LOG(ERROR) << __FUNCTION__ << " is too busy!";
            }
        }
        last_time_us = butil::gettimeofday_us();
        
        list_all(&list);
        while (!list.empty()) {
            PlayerGroup* players = list.back().second.get();
            players->update_stats_every_second();
            list.pop_back();
        }
    }
}

void* RtmpForwardService::run_dump_stats(void* arg) {
    static_cast<RtmpForwardService*>(arg)->dump_stats();
    return NULL;
}

struct StreamEntry {
    void* players;
    int64_t created_time;
};

struct PlayerEntry {
    const PlayerInfo* player;
    int64_t created_time;
};

struct PublisherEntry {
    const RtmpForwarder* publisher;
    int64_t created_time;
};

static bool operator==(const StreamEntry& s1, const StreamEntry& s2) {
    return s1.players == s2.players && s1.created_time == s2.created_time;
}

static bool operator==(const PlayerEntry& p1, const PlayerEntry& p2) {
    return p1.player == p2.player && p1.created_time == p2.created_time;
}

static bool operator==(const PublisherEntry& p1, const PublisherEntry& p2) {
    return p1.publisher == p2.publisher && p1.created_time == p2.created_time;
}

namespace BUTIL_HASH_NAMESPACE {
template <>
struct hash<StreamEntry> {
    std::size_t operator()(const StreamEntry& s) const {
        return butil::HashInts64((uint64_t)s.players, s.created_time);
    }
};

template <>
struct hash<PlayerEntry> {
    std::size_t operator()(const PlayerEntry& p) const {
        return butil::HashInts64((uint64_t)p.player, p.created_time);
    }
};

template <>
struct hash<PublisherEntry> {
    std::size_t operator()(const PublisherEntry& p) const {
        return butil::HashInts64((uint64_t)p.publisher, p.created_time);
    }
};
}

struct StreamStates {
    size_t video_frames;
    size_t video_bytes;
    size_t audio_frames;
    size_t audio_bytes;
    int64_t peek_time;
};

struct PlayerStates {
    size_t sent_video_bytes;
    size_t sent_audio_bytes;
    size_t sent_video_frames;
    int64_t peek_time;
};

struct PublisherStates {
    size_t received_video_bytes;
    size_t received_audio_bytes;
    size_t received_video_frames;
    int64_t peek_time;
};

// Add the time prefix to each line of stream stats so that all stats of a
// stream can be viewed and sorted (by time) in kibana 
static std::string MakeStatsPrefix(int64_t microseconds) {
    time_t t = microseconds / 1000000L;
    struct tm local_tm = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL};
#if _MSC_VER >= 1400
    localtime_s(&local_tm, &t);
#else
    localtime_r(&t, &local_tm);
#endif
    char buf[64];
    int len = 0;
    buf[len++] = 'T';
    int ret = snprintf(buf + len, sizeof(buf) - len,
                       "%04d%02d%02d %02d:%02d:%02d.%06d [STATS] ",
                       local_tm.tm_year + 1900,
                       local_tm.tm_mon + 1,
                       local_tm.tm_mday,
                       local_tm.tm_hour,
                       local_tm.tm_min,
                       local_tm.tm_sec,
                       (int)(microseconds - t * 1000000L));
    len += ret;
    return std::string(buf, len);
}

bool is_guess_expected_framerate(double* expected_framerate, double real_framerate) {
    bool is_guess_result = false;
    if (*expected_framerate <= 0 && real_framerate > 0) {
        is_guess_result = true;
        if (real_framerate <= 22) {
            *expected_framerate = 15;
        } else {
            *expected_framerate = 30;
        }
    }
    return is_guess_result;
}

size_t get_score(double* expected_framerate, double real_framerate) {
    is_guess_expected_framerate(expected_framerate, real_framerate);
    size_t score = 100;
    if (*expected_framerate > 0) {
        score = (size_t)(real_framerate * 100 / *expected_framerate);
    }
    return score;
}

void RtmpForwardService::dump_stats() {
    butil::FlatMap<StreamEntry, StreamStates> states_map;
    CHECK_EQ(0, states_map.init(256, 70));
    std::vector<std::pair<StreamEntry, StreamStates> > new_st_to_insert;
    new_st_to_insert.reserve(256);
    std::vector<std::pair<std::string, PlayerGroupSharedPtr> > groups;
    std::vector<butil::intrusive_ptr<PlayerInfo> > infos;

    while (!bthread_stopped(bthread_self())) {
        bthread_usleep(FLAGS_dump_stats_interval * 1000000L);
        butil::IOBufBuilder os;
        list_all(&groups);
        if (groups.empty()) {
            continue;
        }
        size_t max_key_length = 10;
        for (size_t i = 0; i < groups.size(); ++i) {
            max_key_length = std::max(groups[i].first.size(), max_key_length);
        }
        const std::string prefix = MakeStatsPrefix(butil::gettimeofday_us());
        os << prefix << "CreatedTime               |From               |Key";
        for (size_t i = 3; i < max_key_length; ++i) {
            os << ' ';
        }
        os << "|Gop |F/s|Video B/s|Audio B/s|Fluency|Play|\n";
        for (size_t i = 0; i < groups.size(); ++i) {
            const std::string& key = groups[i].first;
            PlayerGroup* players = groups[i].second.get();
            const size_t player_count = players->count();
            const StreamEntry e = { players, players->_create_realtime_us };
            const StreamStates* st = states_map.seek(e);
            StreamStates last_st = StreamStates();
            if (st) {
                last_st = *st;
            } else {
                last_st.peek_time = players->_create_realtime_us;
            }
            const StreamStates cur_st = {
                players->_video_frames,
                players->_video_bytes,
                players->_audio_frames,
                players->_audio_bytes,
                butil::gettimeofday_us()
            };
            new_st_to_insert.push_back(std::make_pair(e, cur_st));
            states_map[e] = last_st;
            size_t video_bytes_per_second = 1000000L *
                (cur_st.video_bytes - last_st.video_bytes) /
                (cur_st.peek_time - last_st.peek_time);
            size_t audio_bytes_per_second = 1000000L *
                (cur_st.audio_bytes - last_st.audio_bytes) /
                (cur_st.peek_time - last_st.peek_time);
            
            butil::intrusive_ptr<RtmpForwarder> publisher = players->_publisher;
            const size_t actual_framerate =
                (size_t)round(1000000.0 * (cur_st.video_frames - last_st.video_frames) /
                              (cur_st.peek_time - last_st.peek_time));
            double expected_framerate = 0;
            if (publisher) {
                expected_framerate = publisher->_expected_framerate;
            }
            char fluency_str[8];
            bool add_star = is_guess_expected_framerate(&expected_framerate, actual_framerate);
            if (expected_framerate > 0) {
                int len = snprintf(fluency_str, sizeof(fluency_str), "%.1f%%",
                                   100 * actual_framerate / expected_framerate);
                if (add_star) {
                    fluency_str[len] = '*';
                    fluency_str[len + 1] = '\0';
                    ++len;
                }
            } else {
                fluency_str[0] = '-';
                fluency_str[1] = '\0';
            }
            os << prefix
               << brpc::PrintedAsDateTime(players->_create_realtime_us) << '|'
               << brpc::min_width(players->remote_side(), 19) << '|'
               << brpc::min_width(key, max_key_length) << '|'
               << brpc::min_width(players->_last_gop_size, 4) << '|'
               << brpc::min_width(actual_framerate, 3) << '|'
               << brpc::min_width(video_bytes_per_second, 9) << '|'
               << brpc::min_width(audio_bytes_per_second, 9) << '|'
               << brpc::min_width(fluency_str, 7) << '|'
               << brpc::min_width(player_count, 4) << '|';
            
            players->list_all(&infos);
            bool first_print = true;
            for (size_t i = 0; i < infos.size(); ++i) {
                PlayerInfo* info = infos[i].get();
                const size_t stuck_nvideo = info->stuck_video_times -
                    info->stuck_video_times_stored_by_dump_stats;
                const size_t stuck_naudio = info->stuck_audio_times -
                    info->stuck_audio_times_stored_by_dump_stats;
                if (stuck_nvideo == 0 && stuck_naudio == 0) {
                    continue;
                }
                if (first_print) {
                    first_print = false;
                    os << " stuck:";
                }
                const char seps[] = "( ";
                int sep_index = 0;
                os << ' ' << info->stream->remote_side() << '['
                   << info->stream->stream_id() << ']';
                if (stuck_nvideo != 0) {
                    os << seps[sep_index++] << "v=" << stuck_nvideo;
                }
                if (stuck_naudio != 0) {
                    os << seps[sep_index++] << "a=" << stuck_naudio;
                }
                os << ')';
                info->stuck_video_times_stored_by_dump_stats += stuck_nvideo;
                info->stuck_audio_times_stored_by_dump_stats += stuck_naudio;
            }
            os << '\n';
        }
        LOG(INFO) << "Stream stats (" << groups.size() << " in total):\n"
                  << os.buf() << "--------END--------";
        // Refresh states_map
        states_map.clear();
        for (size_t i = 0; i < new_st_to_insert.size(); ++i) {
            states_map[new_st_to_insert[i].first] = new_st_to_insert[i].second;
        }
        new_st_to_insert.clear();
        groups.clear();
        infos.clear();
    }
}

void print_publisher_stats(const std::string& key,
                           const RtmpForwarder* publisher,
                           const std::string& publish_type,
                           const butil::EndPoint& remote_ip,
                           double* expected_framerate,
                           const PublisherStates& cur_publisher_states,
                           butil::FlatMap<PublisherEntry, PublisherStates>* publisher_states_map,
                           std::vector<std::pair<PublisherEntry, PublisherStates>>* new_publisher_states_to_insert) {
    const PublisherEntry publisher_entry = {publisher, publisher->create_realtime_us()};
    const PublisherStates* publisher_states = publisher_states_map->seek(publisher_entry);
    PublisherStates last_publisher_states = PublisherStates();
    if (publisher_states) {
        last_publisher_states = *publisher_states;
    } else {
        last_publisher_states.peek_time = publisher->create_realtime_us();
    }
    new_publisher_states_to_insert->push_back(std::make_pair(publisher_entry, cur_publisher_states));
    (*publisher_states_map)[publisher_entry] = last_publisher_states;
    size_t time_elapsed = cur_publisher_states.peek_time - last_publisher_states.peek_time;
    size_t video_bits_per_second = 8 * 1000000L * 
       (cur_publisher_states.received_video_bytes - last_publisher_states.received_video_bytes) / time_elapsed;
    size_t audio_bits_per_second = 8 * 1000000L * 
       (cur_publisher_states.received_audio_bytes - last_publisher_states.received_audio_bytes) / time_elapsed;
    size_t video_frames_per_second = 1000000L * 
       (cur_publisher_states.received_video_frames - last_publisher_states.received_video_frames) / time_elapsed;
    size_t score = get_score(expected_framerate, video_frames_per_second);
    LOG(INFO) << "[CONN_STATS]"
              << publish_type
              << "session:" << key << ", " 
              << "ip:" << remote_ip << ", "
              << "flowrate:" <<  video_bits_per_second + audio_bits_per_second << ", "
              << "videorate:" << video_bits_per_second << ", "
              << "audiorate:" << audio_bits_per_second << ", "
              << "videofps:" << video_frames_per_second << ", "
              << "score:" << score;

}

void print_player_stats(const std::string& key,
                        double* expected_framerate,
                        const PlayerInfo* info,
                        butil::FlatMap<PlayerEntry, PlayerStates>* last_player_states_map,
                        std::vector<std::pair<PlayerEntry, PlayerStates>>* new_ps_to_insert) {
    if (dynamic_cast<TsQueue*>(info->stream.get())) {
        return;
    }
    const PlayerEntry player_entry = {info, info->stream->create_realtime_us()};
    const PlayerStates* player_states = last_player_states_map->seek(player_entry);
    PlayerStates last_player_states = PlayerStates();
    if (player_states) {
        last_player_states = *player_states;
    } else {
        last_player_states.peek_time = info->stream->create_realtime_us();
    }
    const PlayerStates cur_player_states = {
        info->sent_video_bytes,
        info->sent_audio_bytes,
        info->sent_video_frames,
        butil::gettimeofday_us()
    };
    new_ps_to_insert->push_back(std::make_pair(player_entry, cur_player_states));
    (*last_player_states_map)[player_entry] = last_player_states;
    size_t time_elapsed = cur_player_states.peek_time - last_player_states.peek_time;
    size_t video_bits_per_second = 8 * 1000000L *
       (cur_player_states.sent_video_bytes - last_player_states.sent_video_bytes) / time_elapsed;
    size_t audio_bits_per_second = 8 * 1000000L *
       (cur_player_states.sent_audio_bytes - last_player_states.sent_audio_bytes) / time_elapsed;
    size_t video_frames_per_second = 1000000L *
       (cur_player_states.sent_video_frames - last_player_states.sent_video_frames) / time_elapsed;
    size_t score = get_score(expected_framerate, video_frames_per_second);
    std::string play_type;
    if (dynamic_cast<RtmpForwarder*>(info->stream.get())) {
        play_type = "play stats, ";
    } else if (dynamic_cast<PublishProxy*>(info->stream.get())) {
        play_type = "forward stats, ";
    } else if (dynamic_cast<FlvDownloader*>(info->stream.get())) {
        play_type = "play_flv stats, ";
    } 
    LOG(INFO) << "[CONN_STATS]"
              << play_type
              << "session:" << key << ", " 
              << "ip:" << info->stream->remote_side() << ", "
              << "flowrate:" <<  video_bits_per_second + audio_bits_per_second << ", "
              << "videorate:" << video_bits_per_second << ", "
              << "audiorate:" << audio_bits_per_second << ", "
              << "videofps:" << video_frames_per_second << ", "
              << "score:" << score;
}

void RtmpForwardService::print_stats() {
    butil::FlatMap<StreamEntry, butil::FlatMap<PlayerEntry, PlayerStates>> player_states_map;
    CHECK_EQ(0, player_states_map.init(256, 70));
    std::vector<std::pair<std::string, PlayerGroupSharedPtr>> groups;
    std::vector<std::pair<StreamEntry, butil::FlatMap<PlayerEntry, PlayerStates>>> new_player_states_to_insert;
    butil::FlatMap<PublisherEntry, PublisherStates> publisher_states_map;
    CHECK_EQ(0, publisher_states_map.init(256, 70));
    std::vector<std::pair<PublisherEntry, PublisherStates>> new_publisher_states_to_insert;

    while (!bthread_stopped(bthread_self())) {
        bthread_usleep(10 * 1000000L);
        list_all(&groups);
        if (groups.empty()) {
            continue;
        }
        for (size_t i = 0; i < groups.size(); ++i) {
            const std::string& key = groups[i].first;
            PlayerGroup* const players = groups[i].second.get();
            const StreamEntry stream_entry = {players, players->_create_realtime_us};
            const butil::intrusive_ptr<RtmpForwarder> publisher = players->_publisher;
            const butil::intrusive_ptr<PlayerAsPublisher> puller = players->_puller;
            double expected_framerate = publisher ? publisher->_expected_framerate : 0;
            std::string publish_type;
            butil::EndPoint remote_ip;
            if (puller) {    
                publish_type = "ingester stats, ";
                remote_ip = puller->remote_side();
            } else if (publisher) {
                publish_type = "publish stats, ";
                remote_ip = publisher->remote_side();
            }
            if (publisher) {   
                const PublisherStates cur_publisher_states = {
                    publisher->_received_video_bytes,
                    publisher->_received_audio_bytes,
                    publisher->_received_video_frames,
                    butil::gettimeofday_us()
                };
                print_publisher_stats(key,  publisher.get(),
                                      publish_type, remote_ip,
                                      &expected_framerate,
                                      cur_publisher_states,
                                      &publisher_states_map, 
                                      &new_publisher_states_to_insert);
            }
            butil::FlatMap<PlayerEntry, PlayerStates>* players_map = player_states_map.seek(stream_entry);
            butil::FlatMap<PlayerEntry, PlayerStates> last_player_states_map;
            CHECK_EQ(0, last_player_states_map.init(32, 70));
            if (players_map) {
                last_player_states_map = *players_map;
            }
            std::vector<butil::intrusive_ptr<PlayerInfo>> infos;
            players->list_all(&infos);
            if (infos.empty()) {
                continue;
            }
            std::vector<std::pair<PlayerEntry, PlayerStates>> new_ps_to_insert;
            for (size_t i = 0; i < infos.size(); ++i) {
                PlayerInfo* info = infos[i].get();
                print_player_stats(key, &expected_framerate, info, &last_player_states_map, &new_ps_to_insert);
            }
            last_player_states_map.clear();
            for (size_t i = 0; i < new_ps_to_insert.size(); ++i) {
                last_player_states_map[new_ps_to_insert[i].first] = new_ps_to_insert[i].second;
            }
            new_ps_to_insert.clear();
            infos.clear();
            new_player_states_to_insert.push_back(std::make_pair(stream_entry, last_player_states_map));
        }
        publisher_states_map.clear();
        for (size_t i = 0; i < new_publisher_states_to_insert.size(); ++i) {
            publisher_states_map[new_publisher_states_to_insert[i].first] = new_publisher_states_to_insert[i].second;
        }
        new_publisher_states_to_insert.clear();
        player_states_map.clear();
        for (size_t i = 0; i < new_player_states_to_insert.size(); ++i) {
            player_states_map[new_player_states_to_insert[i].first] = new_player_states_to_insert[i].second;
        }
        new_player_states_to_insert.clear();
        groups.clear();
    }
}
 
RtmpForwardService::RtmpForwardService()
    : _has_update_stats_thread(false)
    , _has_dump_stats_thread(false)
    , _has_print_stats_thread(false)
    , _has_remove_idle_group_thread(false) {
    pthread_mutex_init(&_forward_map_mutex, NULL);
}

int RtmpForwardService::init(const RtmpForwardServiceOptions& options) {
    if (options.port < 0) {
        LOG(ERROR) << "Invalid port=" << options.port;
        return -1;
    }
    _options = options;
    if (bthread_start_background(&_update_stats_thread, NULL,
                                 run_update_stats, this) != 0) {
        LOG(ERROR) << "Fail to create _update_stats_thread";
        return -1;
    }
    _has_update_stats_thread = true;

    if (FLAGS_dump_stats_interval > 0) {
        if (bthread_start_background(&_dump_stats_thread, NULL,
                                     run_dump_stats, this) != 0) {
            LOG(ERROR) << "Fail to create _dump_stats_thread";
            return -1;
        }
        _has_dump_stats_thread = true;
    }
    
    if (FLAGS_log_stats) {
        if (bthread_start_background(&_print_stats_thread, NULL,
                                      run_print_stats, this) != 0) {
            LOG(ERROR) << "Fail to create _print_stats_thread";
            return -1;
        }
        _has_print_stats_thread = true;
    }

    if (FLAGS_keep_pulling_seconds) {
        if (bthread_start_background(&_remove_idle_group_thread, NULL,
                                    run_remove_idle_group, this) != 0) {
            LOG(ERROR) << "Fail to create _remove_idle_group_thread";
            return -1;
        }
        _has_remove_idle_group_thread = true;
    }

    return 0;
}

int RtmpForwardService::set_user_config_in_frame_queue(
    const std::string& key,
    FrameQueueUserConfig* config) {
    std::unique_ptr<FrameQueueUserConfig> delete_config(config);
    PlayerGroupSharedPtr players;
    find_player_group(key, &players);
    if (players == NULL) {
        LOG(WARNING) << "Fail to set user config in frame_queue, key=" << key;
        return -1;
    }
    players->frame_queue.set_user_config(delete_config.release());
    return 0;
}

struct IsPlayGroupIdle {
    IsPlayGroupIdle() : _now_ms(butil::gettimeofday_ms()) {}
    bool operator() (PlayerGroup& val) const {
        return (val.start_idle_time_ms() + FLAGS_keep_pulling_seconds * 1000) < _now_ms;
    }
private:
    int64_t _now_ms;
};

void* RtmpForwardService::run_remove_idle_group(void* arg) {
    static_cast<RtmpForwardService*>(arg)->remove_idle_group();
    return NULL;
}

void* RtmpForwardService::remove_idle_group() {
    std::vector<std::pair<std::string, butil::intrusive_ptr<PlayerGroup> > > removals;
    while (!bthread_stopped(bthread_self())) {
        IsPlayGroupIdle filter;
        _idle_group_map.remove_by(filter, &removals);
        for (size_t i = 0; i < removals.size(); ++i) {
            PlayerAsPublisher* swapped_puller = NULL;
            PlayerGroupSharedPtr swapped_players;
            const std::string& key = removals[i].first;
            butil::intrusive_ptr<PlayerGroup> & players = removals[i].second;
            {
                std::unique_lock<pthread_mutex_t> mu(_forward_map_mutex);
                ForwardMap::iterator it = _forward_map.find(key);
                if (it == _forward_map.end()) {
                    continue;
                }
                {
                    std::unique_lock<pthread_mutex_t> mu2(players->_member_mutex);
                    // playergroup only have reference in removals/_forward_map/local_publisher
                    if (players->_list.empty()) {
                        // Destroy the client stream when there're no players for more than 20s. 
                        // Notice that the dtor sends command and is not proper to be done inside lock
                        swapped_puller = players->_puller;
                        players->_puller = NULL;
                        // Remove the entry when publisher is absent as well.
                        if (players->_publisher == NULL) {
                            players->_has_removed = true;
                            players.swap(swapped_players);
                            _forward_map.erase(it);
                        }
                    }
                }
            }
            if (swapped_puller) {
                swapped_puller->Destroy();
            }
        }
        removals.clear();
        bthread_usleep(1000000/*1s*/);
    }
    return NULL;
}

RtmpForwardService::~RtmpForwardService() {
    pthread_mutex_destroy(&_forward_map_mutex);
    if (_has_update_stats_thread) {
        bthread_stop(_update_stats_thread);
        bthread_join(_update_stats_thread, NULL);
    }
    if (_has_dump_stats_thread) {
        bthread_stop(_dump_stats_thread);
        bthread_join(_dump_stats_thread, NULL);
    }

    if (_has_print_stats_thread) {
        bthread_stop(_print_stats_thread);
        bthread_join(_print_stats_thread, NULL);
    }

    if (_has_remove_idle_group_thread) {
        bthread_stop(_remove_idle_group_thread);
        bthread_join(_remove_idle_group_thread, NULL);
    }
}

brpc::RtmpServerStream* RtmpForwardService::NewStream(
    const brpc::RtmpConnectRequest& conn_req) {
    return new RtmpForwarder(this, conn_req, true);
}

class PlayingClientSelector : public RtmpClientSelector {
public:
    PlayingClientSelector(const std::string& key,
                          RtmpForwardService* service, 
                          const std::string& queries,
                          bool simplified_rtmp)
        : _key(key)
        , _service(service)
        , _queries(queries)
        , _simplified_rtmp(simplified_rtmp) {}
    // @RtmpClientSelector
    void StartGettingRtmpClient(OnGetRtmpClient* done);
private:
    const std::string _key;
    RtmpForwardService* const _service;
    const std::string _queries;
    const bool _simplified_rtmp;
};

void PlayingClientSelector::StartGettingRtmpClient(
    OnGetRtmpClient* done) {
    butil::StringPiece vhost;
    butil::StringPiece app;
    butil::StringPiece stream_name;
    brpc::ParseRtmpURL(_key, &vhost, NULL, NULL, &app, &stream_name);
    _service->get_proxy_client(vhost, app, stream_name, _queries, false, _simplified_rtmp, done);
}

static PullWay select_pull_way(const butil::StringPiece& key,
                               bool is_internal,
                               std::shared_ptr<QueryModifier>* modifier) {
    return PULL_BY_RTMP_INSIDE;
}    

static FrameIndicator get_frame_indicator(const std::string& key,
                                          const butil::StringPiece& queries,
                                          RtmpForwardService* service) {
    FrameIndicator indicator;
    // There are four types of FrameIndicator. Read the class declaration for details
    return indicator;
}

// NOTE: `player' could be NULL to trigger creation of puller only.
butil::Status RtmpForwardService::add_player(const std::string& key,
                                            const butil::StringPiece& queries,
                                            brpc::RtmpStreamBase* player,
                                            int64_t play_delay,
                                            bool keep_original_time) {
    if (player != NULL && player->is_stopped()) {
        // This is a pre-check for the strict checking below inside the lock.
        return butil::Status(EINVAL, "player from %s on %s was already stopped",
                            butil::endpoint2str(player->remote_side()).c_str(),
                            key.c_str());
    }
    PlayerGroupSharedPtr players;
    if (_options.proxy_to.empty()) {
        // In aggregating mode, players are rejected immediately if the
        // stream is not found, which makes the clients retry faster.
        find_player_group(key, &players);
    } else {
        // In proxy mode, pulling is triggered by the first player, when
        // the stream does not exist definitely, thus we can't reject a player
        // due to exist-ness of the stream. Actually we don't reject players
        // until the pulling fails for at most -max_retry_duration_ms
        // during which the stream may be pulled for multiple times.
        find_or_new_player_group(key, &players,
                                 (player ? player->create_realtime_us() : 0),
                                 play_delay);
    }
    if (players == NULL) {
        return butil::Status(EPERM, "Fail to find key=%s", key.c_str());
    }
    butil::intrusive_ptr<PlayerInfo> info;
    if (player != NULL) {
        info.reset(new PlayerInfo());
        info->stream.reset(player);
        info->cursor = new DefaultCursor(&players->frame_queue, get_frame_indicator(key, queries, this));
        info->keep_original_time = keep_original_time;
        // check if player is external client
        info->is_outtest_cdn = is_outtest_cdn(queries);
        // Flush existing frames immediately. Require frame_queue to be thread-safe.
        info->flush();
    }
    bool create_puller = false;
    {
        std::unique_lock<pthread_mutex_t> mu(players->_member_mutex);
        if (players->_has_removed) {
            // Here playergroup has been removed from _forward_map, thus 
            // add_player next time will find and new playergroup in _forward_map 
            // and has_removed will be reset to false
            mu.unlock();
            return add_player(key, queries, player, play_delay, keep_original_time);
        }
        if (player != NULL) {
            if (player->is_stopped()) {
                // player->OnStop() was already called. RtmpForwarder may be
                // destroyed and removed from players before being added when
                // creation of the stream is asynchronous. We're assume that
                // remove_player is called inside OnStop() thus if remove_player
                // happens before this add_player, is_stopped() must be true.
                // Memory fences are OK due to _member_mutex.
                mu.unlock();
                // Remove the entry in _forward_map when where's no players.
                remove_player(key, NULL/*note*/);
                return butil::Status(EINVAL, "The player was already stopped");
            }
            players->_list.push_back(info);
            if (FLAGS_keep_pulling_seconds || 
                _options.proxy_to.empty()) {
                players->_start_idle_time_ms = 0;
            }
        }
        if (!_options.proxy_to.empty() &&
            players->_puller == NULL &&
            !players->_creating_puller &&
            !is_publish_proxy(key)) {
            // Set creating_puller to true to let only one player start
            // creating the puller.
            players->_creating_puller = true;
            create_puller = true;
        }
    }
    if (!create_puller) {
        return butil::Status::OK();
    }
    
    std::string new_queries(queries.data(), queries.size());

    butil::StringPiece vhost;
    butil::StringPiece app;
    butil::StringPiece stream_name;
    brpc::ParseRtmpURL(key, &vhost, NULL, NULL, &app, &stream_name);
    std::shared_ptr<QueryModifier> query_modifier;
    PullWay pull_way = select_pull_way(key, options().internal_service, &query_modifier);
    VLOG(99) << "pull_way=" << pull_way;
    if (query_modifier) {
        query_modifier->modify(&new_queries);
    }
    std::vector<std::string> servers;
    std::string port;
    bool initialized = false;
    int rc = 0;
    brpc::DestroyingPtr<PlayerAsPublisher> new_puller;
    brpc::SubStreamCreator* sub_stream_creator = NULL;
    // If new_puller was moved to players->puller successfully, it should be
    // empty, otherwise error occurred and new_puller should be destroyed.
    // NOTE: we don't retry the creation of puller right now, remaining players
    // should see black screens, until new players come in and start creating
    // the puller again.
    brpc::RtmpConnectRequest conn_req;
    conn_req.set_app(app.as_string());
    conn_req.set_tcurl(brpc::MakeRtmpURL(vhost, "", app, ""));
    RtmpForwarder* local_publisher =
        new RtmpForwarder(this, conn_req, false/*not rtmp user*/);
    if (local_publisher == NULL) {
        LOG(ERROR) << "Fail to create local_publisher";
        goto SET_PULLER_IN_PLAYERS;
    }
    switch (pull_way) {
    case PULL_BY_RTMP_INSIDE:
    case PULL_BY_RTMP_OUTSIDE:
        new_puller.reset(new PlayerAsPublisher(this, key));
        sub_stream_creator = new RtmpSubStreamCreator(new PlayingClientSelector(
                key, this, new_queries, FLAGS_simplified_rtmp_play && pull_way == PULL_BY_RTMP_INSIDE),
                options().internal_service);
        break;
    default:
        LOG(ERROR) << "Unknown pulling type=" << pull_way;
        goto SET_PULLER_IN_PLAYERS;
    }
    rc = new_puller->Init(sub_stream_creator, key, vhost, app, stream_name, new_queries, local_publisher);
    if (rc != 0) {
        LOG(ERROR) << "Fail to init puller=" << new_puller.get() << " for "
                   << conn_req.tcurl() << '/' << stream_name << noflush;
        if (!new_queries.empty()) {
            LOG(ERROR) << '?' << new_queries;
        } else {
            LOG(ERROR);
        }
        goto SET_PULLER_IN_PLAYERS;
    }
    VLOG(99) << "Creating puller=" << new_puller.get() << " of " << key;
    initialized = true;
SET_PULLER_IN_PLAYERS:
    {
        std::unique_lock<pthread_mutex_t> mu(players->_member_mutex);
        if (players->_puller == NULL) {
            players->_creating_puller = false;
            if (initialized && !players->_list.empty()) {
                const void* new_puller_ptr = new_puller.get();
                players->_puller = new_puller.release();
                mu.unlock();
                if (!new_queries.empty()) {
                    VLOG(99) << "Set puller=" << new_puller_ptr << " of "
                             << conn_req.tcurl() << '/' << stream_name
                             << '?' << queries;
                } else {
                    VLOG(99) << "Set puller=" << new_puller_ptr << " of "
                             << conn_req.tcurl() << '/' << stream_name;
                }
            } // else no players anymore, the puller is not needed right now.
        }
    }
    return butil::Status::OK();    
}

int64_t RtmpForwardService::get_player_count(const std::string& key) {
    PlayerGroupSharedPtr players;
    find_player_group(key, &players);
    if (players == NULL) {
        return -1;
    }
    return players->count();
}

void RtmpForwardService::remove_player(
    const std::string& key, const brpc::RtmpStreamBase* stream) {
    PlayerAsPublisher* swapped_puller = NULL;
    PlayerGroupSharedPtr swapped_players;
    butil::intrusive_ptr<PlayerInfo> swapped_player_info;
    {
        std::unique_lock<pthread_mutex_t> mu(_forward_map_mutex);
        ForwardMap::iterator it = _forward_map.find(key);
        if (it == _forward_map.end() || it->second == NULL) {
            return;
        }
        PlayerGroupSharedPtr& players = it->second;
        std::unique_lock<pthread_mutex_t> mu2(players->_member_mutex);
        std::vector<butil::intrusive_ptr<PlayerInfo> >& list = players->_list;
        if (stream != NULL) {
            // TODO: May need opt. for #players > 1k
            for (size_t i = 0; i < list.size(); ++i) {
                if (list[i]->stream.get() == stream) {
                    // Destruct list[i] outside lock to avoid deadlock. e.g.
                    // dtor of the stream may call count_players which locks
                    // _member_mutex as well.
                    list[i].swap(swapped_player_info);
                    list[i].swap(list.back());
                    list.pop_back();
                    break;
                }
            }
        }
        // Record the last cdn external or inner player's time of removal 
        // to determine the time playergroup destroies
        if (swapped_player_info != NULL && 
            (swapped_player_info->is_outtest_cdn || _options.proxy_to.empty())) {
            players->_start_idle_time_ms = butil::gettimeofday_ms();
        }
        if (list.empty()) {
            // cdn merge service will not keep ingestion
            if (FLAGS_keep_pulling_seconds && !_options.internal_service) {
                _idle_group_map.put(key, players);
            } else {
                // Destroy the client stream when there're no players. Notice that
                // the dtor sends command and is not proper to be done inside lock
                swapped_puller = players->_puller;
                players->_puller = NULL;
                // Remove the entry when publisher is absent as well.
                if (players->_publisher == NULL) {
                    players->_has_removed = true;
                    players.swap(swapped_players);
                    _forward_map.erase(it);
                }
            }
        }   
    }
    if (swapped_puller) {
        swapped_puller->Destroy();
    }
}

size_t RtmpForwardService::count_players(const std::string& key) {
    std::unique_lock<pthread_mutex_t> mu(_forward_map_mutex);
    ForwardMap::iterator it = _forward_map.find(key);
    if (it == _forward_map.end() || it->second == NULL) {
        return 0;
    }
    return it->second->count();
}

int RtmpForwardService::add_publisher(const std::string& key,
                                      RtmpForwarder* publisher,
                                      PlayerGroupSharedPtr* players_out,
                                      int64_t publish_delay) {
    PlayerGroupSharedPtr players;
    find_or_new_player_group(key, &players, publisher->create_realtime_us(),
                             publish_delay);
    if (players == NULL) {
        LOG(ERROR) << "Fail to get PlayerGroup(" << key;
        return -1;
    }
    // Newer publisher may come in before older publisher quits
    // (connection broken), we provide two ways for this situation.
    // 1. When FLAGS_reject_second_stream_when_republish set, 
    // reject the second publish stream and terminate the second connection.
    // 2. To make the new publisher work immediately, just
    // kick off the older publisher. One side effect of this
    // solution is that a well-working publisher is possibly interrupted by
    // another publisher(accidentally or intentionally) as well.
    butil::intrusive_ptr<RtmpForwarder> old_publisher;
    bool reject_second_publisher = false;
    {
        std::unique_lock<pthread_mutex_t> mu(players->_member_mutex);
        if (FLAGS_reject_second_stream_when_republish && (players->_publisher != NULL)) {
            reject_second_publisher = true;
            old_publisher = players->_publisher;
        } else { 
            if (players->_publisher != NULL) {
                old_publisher.swap(players->_publisher);
            }
            players->_publisher.reset(publisher);
            players->frame_queue.reset(publisher);
            players_out->swap(players);
        }
    }
    if (reject_second_publisher) {
        LOG(WARNING) << publisher << '@' << publisher->remote_side()
                     << " try replacing " << old_publisher.get() << '@' 
                     << old_publisher->remote_side() 
                     << " is publishing " << key;
        publisher->SendStopMessage(
            butil::string_printf("Fail to replace publisher %s",
                                butil::endpoint2str(old_publisher->remote_side()).c_str()));
    } else if (old_publisher) {
        LOG(WARNING) << key << " is being published by " << publisher << '@'
                     << publisher->remote_side() << " instead of "
                     << old_publisher.get() << '@'
                     << old_publisher->remote_side();
        // Ending the publisher may trigger its retrying which will replace the
        // new publisher soon, and the new publisher may retry as well, thus
        // the two publishers kick off each other repeatly. Proved for OBS.
        if (FLAGS_stop_kickedoff_publisher) {
            old_publisher->SendStopMessage(
                butil::string_printf("Replaced by publisher from %s",
                                    butil::endpoint2str(publisher->remote_side()).c_str()));
        }
    }
    return reject_second_publisher? -1 : 0;
}

int RtmpForwardService::reset_publisher(const std::string& key,
                                        const RtmpForwarder* publisher) {
    PlayerGroupSharedPtr players;
    find_player_group(key, &players);
    if (players == NULL) {
        LOG(ERROR) << "Fail to get PlayerGroup(" << key << ")";
        return -1;
    }
    std::unique_lock<pthread_mutex_t> mu(players->_member_mutex);
    if (players->_publisher.get() != publisher) {
        // The publisher was dry and kicked by another publisher.
        return -1;
    }
    players->frame_queue.reset(publisher);
    return 0;
}

void RtmpForwardService::remove_publisher(
    const std::string& key, const RtmpForwarder* publisher) {
    PlayerGroupSharedPtr swapped_players;
    std::vector<butil::intrusive_ptr<PlayerInfo> > swapped_list;
    butil::intrusive_ptr<RtmpForwarder> swapped_publisher;
    PlayerAsPublisher* swapped_puller = NULL;
    {
        std::unique_lock<pthread_mutex_t> mu(_forward_map_mutex);
        ForwardMap::iterator it = _forward_map.find(key);
        if (it == _forward_map.end() || it->second == NULL) {
            return;
        }
        std::unique_lock<pthread_mutex_t> mu2(it->second->_member_mutex);
        if (it->second->_publisher.get() != publisher) {
            // The publisher was dry and kicked by another publisher.
            return;
        }
        it->second->_has_removed = true;
        it->second->_list.swap(swapped_list);
        it->second->_publisher.swap(swapped_publisher);
        // puller may reference the PlayerGroup holding it, which prevents
        // ref of PlayerGroup hits zero, break the chain.
        swapped_puller = it->second->_puller;
        it->second->_puller = NULL;
        it->second.swap(swapped_players);
        _forward_map.erase(it);
    }
    if (!swapped_list.empty()) {
        std::string desc = butil::string_printf(
            "publisher on key=%s is gone", key.c_str());
        for (size_t i = 0; i < swapped_list.size(); ++i) {
            swapped_list[i]->stream->SendStopMessage(desc);
        }
    }
    
    if (swapped_puller) {
        swapped_puller->Destroy();
    }
}

void RtmpForwardService::get_proxy_client(const butil::StringPiece& vhost,
                                          const butil::StringPiece& app,
                                          const butil::StringPiece& stream_name,
                                          const butil::StringPiece& queries,
                                          bool is_publish,
                                          bool simplified_rtmp,
                                          OnGetRtmpClient* done) {
    GetOrNewRtmpClient* done2 = new GetOrNewRtmpClient;
    vhost.CopyToString(&done2->vhost);
    app.CopyToString(&done2->app);
    stream_name.CopyToString(&done2->stream);
    done2->done = done;
    done2->is_publish = is_publish;
    done2->simplified_rtmp = simplified_rtmp;
    return get_target_server(vhost, app, stream_name, queries, is_publish, done2);
}

void RtmpForwardService::list_all(
    std::vector<std::pair<std::string, PlayerGroupSharedPtr > >* out) {
    out->clear();
    pthread_mutex_lock(&_forward_map_mutex);
    if (out->capacity() < _forward_map.size()) {
        const size_t sz = _forward_map.size();
        pthread_mutex_unlock(&_forward_map_mutex);
        out->reserve(sz);
        pthread_mutex_lock(&_forward_map_mutex);
    }
    for (ForwardMap::iterator it= _forward_map.begin(); it != _forward_map.end();
         ++it) {
        out->push_back(*it);
    }
    pthread_mutex_unlock(&_forward_map_mutex);
}

void RtmpForwardService::find_player_group(
    const std::string& key,
    PlayerGroupSharedPtr* players_out) {
    players_out->reset();
    {
        std::unique_lock<pthread_mutex_t> mu(_forward_map_mutex);
        ForwardMap::iterator it = _forward_map.find(key);
        if (it != _forward_map.end()) {
            *players_out = it->second;
        }
    }
}

void RtmpForwardService::find_or_new_player_group(
    const std::string& key,
    PlayerGroupSharedPtr* players_out,
    int64_t launcher_create_time_us,
    int64_t first_play_or_publish_delay) {
    players_out->reset();
    {
        std::unique_lock<pthread_mutex_t> mu(_forward_map_mutex);
        PlayerGroupSharedPtr& players = _forward_map[key];
        if (players == NULL) {
            players.reset(new PlayerGroup(key, launcher_create_time_us,
                                          first_play_or_publish_delay));
        }
        *players_out = players;
    }
}

void MonitoringServiceImpl::GetTabInfo(brpc::TabInfoList* info_list) const {
    brpc::TabInfo* info = info_list->add();
    info->tab_name = "Media Server";
    info->path = "/media_server";
}

void RtmpForwardService::get_target_server(const butil::StringPiece& vhost,
                                           const butil::StringPiece& app,
                                           const butil::StringPiece& stream_name,
                                           const butil::StringPiece& queries,
                                           bool is_publish,
                                           OnGetTargetServer* done) {
    // In this function, you can use different value of `options().proxy_to'
    // to decide how to get the target server.
    if (is_publish && at_cdn()) {
        return get_server_to_publish_at_cdn(
            vhost, app, stream_name, done);
    }
    return done->on_get_target_server(butil::Status(),
                                      options().proxy_to.c_str(),
                                      options().proxy_lb.c_str());
}

void RtmpForwardService::get_target_server(const butil::StringPiece& key,
                                           bool is_publish,
                                           OnGetTargetServer* done) {
    butil::StringPiece vhost;
    butil::StringPiece app;
    butil::StringPiece stream_name;
    brpc::ParseRtmpURL(key, &vhost, NULL, NULL, &app, &stream_name);
    return get_target_server(vhost, app, stream_name, butil::StringPiece(), is_publish, done);
}

void RtmpForwardService::get_server_to_publish_at_cdn(
    const butil::StringPiece& vhost,
    const butil::StringPiece& app,
    const butil::StringPiece& stream_name,
    OnGetTargetServer* done) {
    return done->on_get_target_server(butil::Status(),
                                      FLAGS_proxy_to.c_str(),
                                      FLAGS_proxy_lb.c_str());
}

void MonitoringServiceImpl::monitor(
    ::google::protobuf::RpcController* controller_base,
    const HttpRequest* /*request*/,
    HttpResponse* /*response*/,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller_base;
    const bool use_html = brpc::UseHTML(cntl->http_request());
    cntl->http_response().set_content_type(use_html ? "text/html" : "text/plain");
    butil::IOBufBuilder os;
    if (use_html) {
        os << "<!DOCTYPE html><html><head>\n"
           << brpc::gridtable_style()
           << "<script src=\"/js/sorttable\"></script>\n"
           << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
           << brpc::TabsHead() << "</head><body>";
        cntl->server()->PrintTabsBody(os, "Media Server");
    }

    butil::StringPiece proxy_to = rtmp_options().proxy_to;
    if (!proxy_to.empty()) {
        os << "[ proxy_to: " << proxy_to
           << " ]" << (use_html ? "<br>" : "\n");
    }
    const char* const bar = (use_html ? "</td><td>" : "|");
    std::vector<std::pair<std::string, PlayerGroupSharedPtr> > list;
    _forward_service->list_all(&list);
    size_t max_key_length = 10;
    for (size_t i = 0; i < list.size(); ++i) {
        max_key_length = std::max(list[i].first.size(), max_key_length);
    }
    if (use_html) {
        os << "<table class=\"gridtable sortable\" border=\"1\"><tr>"
            "<th>CreatedTime</th>"
            "<th>From</th>"
            "<th>Key</th>"
            "<th>Gop</th>"
            "<th>F/s</th>"
            "<th>Video B/s</th>"
            "<th>Audio B/s</th>"
            "<th>Play</th>"
            "<th>Fluency</th>"
            "<th>Try out</th>"
            "</tr>\n";
    } else {
        os << "CreatedTime               |From               |Key";
        for (size_t i = 3; i < max_key_length; ++i) {
            os << ' ';
        }
        os << "|Gop |F/s|Video B/s|Audio B/s|Play|Fluency|\n";
    }
    for (size_t i = 0; i < list.size(); ++i) {
        if (use_html) {
            os << "<tr><td>";
        }
        const std::string& key = list[i].first;
        PlayerGroup* players = list[i].second.get();
        butil::intrusive_ptr<RtmpForwarder> publisher = players->_publisher;
        float actual_framerate = roundf(players->_video_frames_second.get());
        double expected_framerate = 0;
        if (publisher) {
            expected_framerate = publisher->_expected_framerate;
        }
        char fluency_str[8];
        bool add_star = false;
        if (expected_framerate <= 0 && actual_framerate > 0) {
            add_star = true;
            // take guess of expected_framerate.
            if (actual_framerate <= 22) {
                expected_framerate = 15;
            } else {
                expected_framerate = 30;
            }
        }
        if (expected_framerate > 0) {
            int len = snprintf(fluency_str, sizeof(fluency_str), "%.1f%%",
                               100 * actual_framerate / expected_framerate);
            if (add_star) {
                fluency_str[len] = '*';
                fluency_str[len + 1] = '\0';
                ++len;
            }
        } else {
            fluency_str[0] = '-';
            fluency_str[1] = '\0';
        }
        const size_t cnt = players->count();
        os << brpc::PrintedAsDateTime(players->_create_realtime_us) << bar
           << brpc::min_width(players->remote_side(), 19) << bar
           << brpc::min_width(key, max_key_length) << bar
           << brpc::min_width(players->_last_gop_size, 4) << bar
           << brpc::min_width((uint32_t)actual_framerate, 3) << bar
           << brpc::min_width(players->_video_bytes_last_second, 9) << bar
           << brpc::min_width(players->_audio_bytes_last_second, 9) << bar;
        if (use_html) {
            os << "<a href=\"/players/" << key << "\">" << cnt << "</a>" << bar;
        } else {
            os << brpc::min_width(cnt, 4) << bar;
        }
        os << brpc::min_width(fluency_str, 7) << bar;
        if (use_html) {
            std::string rtmpurl;
            std::string flvurl;
            std::string hlsurl;
            int port = (rtmp_options().internal_port > 0 ?
                        rtmp_options().internal_port : rtmp_options().port);
            key_to_url(key, port, &rtmpurl, &flvurl, &hlsurl);
            os << "<a href=\"http://cyberplayer.bcelive.com/live/index.html"
                "?fileUrl=" << rtmpurl;
            os << "\">rtmp</a> "
                "<a href=\"http://cyberplayer.bcelive.com/live/index.html?fileUrl="
               << flvurl;
            if (!rtmp_options().auth_play.empty()) {
                if (flvurl.find("%3F") != std::string::npos) {
                    os << "%26"; // escaped &
                } else {
                    os << "%3F"; // escaped ?
                }
                os << "magic=" MEDIA_SERVER_AUTH_MAGIC;
            }
            os << "&isLive=true\">flv</a> <a href=\"/play_hls?fileUrl="
               << hlsurl << "\">hls</a></td></tr>";
        }
        os << '\n';
    }
    if (use_html) {
        os << "</table>\n";
        os << "</body></html>";
    }
    os.move_to(cntl->response_attachment());
    cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

void MonitoringServiceImpl::players(
    ::google::protobuf::RpcController* controller_base,
    const HttpRequest* /*request*/,
    HttpResponse* /*response*/,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller_base;
    //const bool use_html = brpc::UseHTML(cntl->http_request());
    cntl->http_response().set_content_type("text/plain");
    const std::string& key = cntl->http_request().unresolved_path();
    PlayerGroupSharedPtr players;
    _forward_service->find_player_group(key, &players);
    butil::IOBufBuilder os;
    if (players == NULL) {
        os << "No players on " << key;
        os.move_to(cntl->response_attachment());
        return;
    }
    FrameQueue::Stats fq_stats;
    players->frame_queue.get_stats(&fq_stats);
    butil::intrusive_ptr<RtmpForwarder> publisher = players->_publisher;
    std::shared_ptr<brpc::RtmpMetaData> saved_metadata;
    std::shared_ptr<brpc::RtmpVideoMessage> saved_avc_seq_header;
    std::shared_ptr<brpc::RtmpAudioMessage> saved_aac_seq_header;
    int64_t publisher_create_time = 0;
    int64_t publish_delay = 0;
    int64_t first_avc_seq_header_delay = 0;
    int64_t first_video_message_delay = 0;
    int64_t first_aac_seq_header_delay = 0;
    int64_t first_audio_message_delay = 0;
    uint32_t publisher_stream_id = 0;
    if (publisher) {
        saved_metadata = publisher->_metadata;
        saved_avc_seq_header = publisher->_avc_seq_header;
        saved_aac_seq_header = publisher->_aac_seq_header;
        publisher_create_time = publisher->create_realtime_us();
        publish_delay = publisher->_play_or_publish_delay;
        first_avc_seq_header_delay = publisher->_first_avc_seq_header_delay;
        first_video_message_delay = publisher->_first_video_message_delay;
        first_aac_seq_header_delay = publisher->_first_aac_seq_header_delay;
        first_audio_message_delay = publisher->_first_audio_message_delay;
        publisher_stream_id = publisher->stream_id();
    }
    os << "[ " << key << " ]"
       << "\nindex=" << fq_stats.max_index
       << "\nremote_side=" << players->remote_side()
       << "\nfirst_player_or_publisher_create="
       << brpc::PrintedAsDateTime(players->_launcher_create_realtime_us)
       << "\nfirst_play_or_publish_delay=" << players->_first_play_or_publish_delay / 1000.0
       << "ms\nplayer_group_create="
       << brpc::PrintedAsDateTime(players->_create_realtime_us)
       << "\npublisher_create="
       << brpc::PrintedAsDateTime(publisher_create_time)
       << "\npublish_delay=" << publish_delay / 1000.0
       << "ms\npublisher_stream_id=" << publisher_stream_id
       << "\nfirst_avc_seq_header_delay=" << first_avc_seq_header_delay / 1000.0
       << "ms\nfirst_video_message_delay=" << first_video_message_delay / 1000.0
       << "ms\nfirst_aac_seq_header_delay=" << first_aac_seq_header_delay / 1000.0
       << "ms\nfirst_audio_message_delay=" << first_audio_message_delay / 1000.0
       << "ms\nlast_push_time=";
    if (fq_stats.last_push_realtime_us != 0) {
        os << brpc::PrintedAsDateTime(fq_stats.last_push_realtime_us)
           << "\nlast_push_to_now="
           << (butil::gettimeofday_us() - fq_stats.last_push_realtime_us) / 1000.0
           << "ms\nlast_message_timestamp=" << fq_stats.last_msg_timestamp;
    } else {
        os << "<never pushed>";
    }
    os << "\ncached=" << fq_stats.nframes
       << "\nkeyframes=" << fq_stats.nkeyframes;
    if (saved_metadata) {
        os << "\nmetadata=" << saved_metadata->data;
    } else {
        os << "\nmetadata=null";
    }
    if (saved_avc_seq_header) {
        os << "\navc_seq_header=" << *saved_avc_seq_header << ' ';
        brpc::RtmpAVCMessage avc_msg;
        butil::Status st;
        st = avc_msg.Create(*saved_avc_seq_header);
        if (!st.ok()) {
            os << st;
        } else {
            brpc::AVCDecoderConfigurationRecord cr;
            st = cr.Create(avc_msg.data);
            if (!st.ok()) {
                os << st;
            } else {
                os << cr;
            }
        }
    } else {
        os << "\navc_seq_header=null";
    }
    if (saved_aac_seq_header) {
        os << "\naac_seq_header=" << *saved_aac_seq_header;
    } else {
        os << "\naac_seq_header=null";
    }
    os << "\n\n";

    std::vector<butil::intrusive_ptr<PlayerInfo> > list;
    players->list_all(&list);
    for (size_t i = 0; i < list.size(); ++i) {
        PlayerInfo* info = list[i].get();
        os << i << ")";
        DefaultCursor* dc = dynamic_cast<DefaultCursor*>(info->cursor);
        if (dc) {
            os << " index=" << dc->last_index();
        }
        if (!info->stream) {
            os << " (null)";
            continue;
        }
        os << ' ' << info->stream->remote_side();
        const brpc::Describable* desc =
            dynamic_cast<const brpc::Describable*>(info->stream.get());
        if (desc) {
            os << ' ';
            desc->Describe(os, brpc::DescribeOptions());
        }
        os << " v=" << info->sent_video_messages
           << " a=" << info->sent_audio_messages
           << " stuck_v=" << info->stuck_video_times
           << " stuck_a=" << info->stuck_audio_times
           << " stuck_u=" << info->stuck_user_times
           << " meta=" << !!info->sent_metadata
           << " vsh=" << !!info->sent_avc_seq_header
           << " ash=" << !!info->sent_aac_seq_header;
        os << " [" << butil::class_name_str(*info->stream) << "]\n";
    }
    os.move_to(cntl->response_attachment());
}

// To get play links easily under console.
void MonitoringServiceImpl::urls(
    ::google::protobuf::RpcController* controller_base,
    const HttpRequest* /*request*/,
    HttpResponse* /*response*/,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = (brpc::Controller*)controller_base;
    //const bool use_html = brpc::UseHTML(cntl->http_request());
    cntl->http_response().set_content_type("text/plain");
    const std::string& key = cntl->http_request().unresolved_path();
    PlayerGroupSharedPtr players;
    _forward_service->find_player_group(key, &players);
    butil::IOBufBuilder os;
    if (players == NULL) {
        os << "No players on " << key;
        os.move_to(cntl->response_attachment());
        return;
    }
    std::string rtmpurl;
    std::string flvurl;
    std::string hlsurl;
    int port = (rtmp_options().internal_port > 0 ?
                rtmp_options().internal_port : rtmp_options().port);
    key_to_url(key, port, &rtmpurl, &flvurl, &hlsurl);
    os << "rtmp: http://cyberplayer.bcelive.com/live/index.html"
        "?fileUrl=" << rtmpurl;
    if (!rtmp_options().auth_play.empty()) {
        os << "?" MEDIA_SERVER_AUTH_MAGIC;
    }
    os << "\nflv:  http://cyberplayer.bcelive.com/live/index.html"
        "?fileUrl=" << flvurl;
    if (!rtmp_options().auth_play.empty()) {
        if (flvurl.find("%3F") != std::string::npos) {
            os << "%26"; // escaped &
        } else {
            os << "%3F"; // escaped ?
        }
        os << "magic=" MEDIA_SERVER_AUTH_MAGIC;
    }
    os << "&isLive=true"
        "\nhls:  http://" << butil::my_hostname() << ':'
       << port << "/play_hls?fileUrl=" << hlsurl << '\n';
    os.move_to(cntl->response_attachment());
}
