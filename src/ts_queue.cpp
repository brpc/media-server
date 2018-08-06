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

#include <gperftools/malloc_extension.h>
#include "butil/memory/singleton_on_pthread_once.h"
#include "butil/fast_rand.h"
#include "bthread/bthread.h"
#include "shared_map.h"
#include "ts_queue.h"

DEFINE_int64(ts_duration_ms, 1042, "Cut a TS every so many milliseconds");
DEFINE_int64(ts_max_stored, 32, "Default max number of TS stored");
// Generate so many "fake" TS after the latest TS so that clients can send
// downloading requests before the TS are truely generated and start
// downloading immediately when the TS has data. This technique relies on
// chunked mode in http 1.1 and hides one RTT between clients and server.
DEFINE_int64(ts_pre_generated, 3, "generate so many fake TS");

static bvar::Adder<int64_t> s_ts_downloader_count("hls_ts_downloader_count");

bool operator==(const TsQueueKey& lhs, const TsQueueKey& rhs) {
    return lhs.key == rhs.key &&
        lhs.started_with_keyframe == rhs.started_with_keyframe;
}

size_t HashTsEntryKey::operator()(const TsEntryKey& key) const {
    uint64_t result[2];
    butil::MurmurHash3_x64_128_Context ctx;
    butil::MurmurHash3_x64_128_Init(&ctx, 0);
    butil::MurmurHash3_x64_128_Update(&ctx, key.key.data(), key.key.size());
    butil::MurmurHash3_x64_128_Update(&ctx, &key.seq, 8);
    butil::MurmurHash3_x64_128_Update(&ctx, &key.started_with_keyframe, 1);
    butil::MurmurHash3_x64_128_Update(&ctx, &key.expiration_ms, 8);
    butil::MurmurHash3_x64_128_Final(&result, &ctx);
    return result[0];
}

size_t HashTsQueueKey::operator()(const TsQueueKey& key) const {
    uint64_t result[2];
    butil::MurmurHash3_x64_128_Context ctx;
    butil::MurmurHash3_x64_128_Init(&ctx, 0);
    butil::MurmurHash3_x64_128_Update(&ctx, key.key.data(), key.key.size());
    butil::MurmurHash3_x64_128_Update(&ctx, &key.started_with_keyframe, 1);
    butil::MurmurHash3_x64_128_Update(&ctx, &key.ts_duration_ms, 8);
    butil::MurmurHash3_x64_128_Update(&ctx, &key.ts_num_per_m3u8, 8);
    butil::MurmurHash3_x64_128_Final(&result, &ctx);
    return result[0];
}

// ========= HLSUserId/HLSUserMeta ==========
HLSUserId generate_hls_user_id() {
    static butil::atomic<uint64_t> userid_base(butil::fast_rand());
    return userid_base.fetch_add(1, butil::memory_order_relaxed);
}

static void* eliminate_hls_user_meta(void*);
static pthread_once_t g_eliminate_hls_user_meta_once = PTHREAD_ONCE_INIT;
static void create_eliminate_hls_user_meta_thread() {
    bthread_t th;
    CHECK_EQ(0, bthread_start_background(
                 &th, NULL, eliminate_hls_user_meta, NULL));
}
typedef SharedMap<HLSUserId, HLSUserMeta> HLSUserMetaMap;
inline HLSUserMetaMap* get_hls_user_meta_map() {
    pthread_once(&g_eliminate_hls_user_meta_once,
                 create_eliminate_hls_user_meta_thread);
    return butil::get_leaky_singleton<HLSUserMetaMap>();
}

bool get_hls_user_meta(HLSUserId user_id, const int64_t expiration_ms,
                       butil::intrusive_ptr<HLSUserMeta>* out) {
    if (get_hls_user_meta_map()->get(user_id, out)) {
        (*out)->expiration_ms = expiration_ms;
        (*out)->mark_as_alive();
        return true;
    }
    return false;
}

bool get_or_new_hls_user_meta(HLSUserId user_id, const int64_t expiration_ms,
                              butil::intrusive_ptr<HLSUserMeta>* out) {
    bool ret = get_hls_user_meta_map()->get_or_new(user_id, out);
    (*out)->expiration_ms = expiration_ms;
    (*out)->mark_as_alive();
    return ret;
}

static size_t get_hls_user_count(void*) {
    HLSUserMetaMap* m = butil::has_leaky_singleton<HLSUserMetaMap>();
    return m != NULL ? m->size() : 0;
}

struct IsHLSUserMetaIdle {
    IsHLSUserMetaIdle() : _now_ms(butil::gettimeofday_ms()) {}
    
    bool operator()(const HLSUserMeta& meta) const
    { return meta.duetime_ms <= _now_ms; }
private:
    int64_t _now_ms;
};
static void* eliminate_hls_user_meta(void*) {
    bvar::PassiveStatus<size_t> hls_user_count(
        "hls_user_count", get_hls_user_count, NULL);
    HLSUserMetaMap* m = get_hls_user_meta_map();
    std::vector<std::pair<HLSUserId, butil::intrusive_ptr<HLSUserMeta> > > removals;
    while (true) {
        IsHLSUserMetaIdle filter;
        m->remove_by(filter, &removals);
        removals.clear();
        bthread_usleep(1000000/*1s*/);
    }
    return NULL;
}

// =========== TsDownloader ==========
TsDownloader::TsDownloader(brpc::ProgressiveAttachment* pa,
                           const butil::IOBuf& unsent_data,
                           bool is_internal,
                           const std::string& key)
    : _pa(pa)
    , _to_be_removed(false)
    , _pa_removed(false)
    , _unsent(unsent_data)
    , _event_log_manager(EVENT_LOG_HLS, is_internal, _pa->remote_side())
    , _sent_bytes(0) 
    , _key(key)
    , _is_internal(is_internal) {
    s_ts_downloader_count << 1;
    _event_log_manager.print_event_log(EVENT_LOG_START_PLAY, _key, _sent_bytes, 0);
}

TsDownloader::~TsDownloader() {
    if (_pa) {
        remove_pa();
    }
    _event_log_manager.print_event_log(EVENT_LOG_STOP_PLAY, _key, _sent_bytes, 0);
    s_ts_downloader_count << -1;
}

void TsDownloader::remove_pa() {
    _pa_removed = true;
    _pa.reset(NULL);
}

int TsDownloader::append_or_flush(const butil::IOBuf* data) {
    if (_unsent.empty()) {
        if (data == NULL) {
            return 0;
        }
        if (_pa->Write(*data) == 0) {
            _sent_bytes += data->size();
            return 0;
        }
        if (errno != brpc::EOVERCROWDED) {
            PLOG(WARNING) << "Fail to write to " << _pa->remote_side();
            return -1;
        }
        _unsent = *data;
        return 0;
    } else {
        if (data) {
            _unsent.append(*data);
        }
        if (_pa->Write(_unsent) == 0) {
            _sent_bytes += _unsent.size();
            _unsent.clear();
            return 0;
        }
        if (errno != brpc::EOVERCROWDED) {
            PLOG(WARNING) << "Fail to write to " << _pa->remote_side();
            return -1;
        }
        return 0;
    }
}

///////////////// FlushDownloadersThread ///////////////

// Flush TsDownloaders that can't be flushed completely when the TsEntry was
// destroyed
class FlushDownloadersThread {
public:
    FlushDownloadersThread();
    ~FlushDownloadersThread();
    int start();
    void stop_and_join();
    void add_downloader(TsDownloader* d);
private:
    static void* run_this(void* arg);
    void run();
    pthread_mutex_t _mutex;
    bthread_t _th;
    std::vector<butil::intrusive_ptr<TsDownloader> > _downloaders;
};

FlushDownloadersThread::FlushDownloadersThread()
    : _th(0) {
    pthread_mutex_init(&_mutex, NULL);
}

FlushDownloadersThread::~FlushDownloadersThread() {
    pthread_mutex_destroy(&_mutex);
}

int FlushDownloadersThread::start() {
    return bthread_start_background(&_th, NULL, run_this, this);
}

void FlushDownloadersThread::stop_and_join() {
    bthread_stop(_th);
    bthread_join(_th, NULL);
}

void* FlushDownloadersThread::run_this(void* arg) {
    static_cast<FlushDownloadersThread*>(arg)->run();
    return NULL;
}

void FlushDownloadersThread::run() {
    VLOG(99) << "FlushDownloadersThread starts";
    std::vector<butil::intrusive_ptr<TsDownloader> > tmp;
    // Save to-be-removed objects to prevent them from destroyed inside lock.
    std::vector<butil::intrusive_ptr<TsDownloader> > removals;
    do {
        {
            std::unique_lock<pthread_mutex_t> mu(_mutex);
            tmp = _downloaders;
        }

        size_t removal_count = 0;
        for (size_t i = 0; i < tmp.size(); ++i) {
            butil::intrusive_ptr<TsDownloader> & d = tmp[i];
            if (d->flush() != 0 || !d->has_unsent()) {
                d->_to_be_removed = true;
                ++removal_count;
            }
        }
        tmp.clear();
        if (removal_count) {
            {
                std::unique_lock<pthread_mutex_t> mu(_mutex);
                for (size_t i = 0; i < _downloaders.size();) {
                    butil::intrusive_ptr<TsDownloader> & d = _downloaders[i];
                    if (d->_to_be_removed) {
                        removals.push_back(d);
                        _downloaders[i] = _downloaders.back();
                        _downloaders.pop_back();
                    } else {
                        ++i;
                    }
                }
            }
            for (size_t i = 0; i < removals.size(); ++i) {
                removals[i]->remove_pa();
            }
            removals.clear();
        }
    } while (bthread_usleep(50000) == 0);
}

void FlushDownloadersThread::add_downloader(TsDownloader* d) {
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    _downloaders.push_back(d);
}

// Get a global singleton of FlushDownloadersThread
struct CreateFlushDownloadersThread : public FlushDownloadersThread {
    CreateFlushDownloadersThread() {
        CHECK_EQ(0, start());
    }
};
inline FlushDownloadersThread* get_flush_downloaders_thread() {
    return butil::get_leaky_singleton<CreateFlushDownloadersThread>();
}

///////////////// TsEntry ///////////////
static bvar::Adder<int64_t> s_ts_entry_count("hls_ts_count");

TsEntry::TsEntry(const std::string& key, int64_t seq_num)
    : _key(key)
    , _seq_num(seq_num)
    , _started_with_keyframe(false)
    , _nkf(0)
    , _start_timestamp(-1)
    , _end_timestamp(-1)
    , _last_active_real_ms(-1) {
    pthread_mutex_init(&_mutex, NULL);
    s_ts_entry_count << 1;
}

TsEntry::TsEntry(const TsEntryKey& ts_entry_key)
    : _key(ts_entry_key.key)
    , _seq_num(ts_entry_key.seq)
    , _started_with_keyframe(false)
    , _nkf(0)
    , _start_timestamp(-1)
    , _end_timestamp(-1)
    , _last_active_real_ms(-1)
    , _expiration_ms(ts_entry_key.expiration_ms) {
    pthread_mutex_init(&_mutex, NULL);
    s_ts_entry_count << 1;
}

TsEntry::~TsEntry() {
    if (!_downloaders.empty()) {
        LOG(ERROR) << *this << " of " << key() << " is destroyed when "
                   << _downloaders.size() << " users are still downloading it";
    }
    pthread_mutex_destroy(&_mutex);
    s_ts_entry_count << -1;
}

void TsEntry::add_downloader(brpc::ProgressiveAttachment* pa, 
                             bool is_internal, 
                             const std::string& charge_key) {
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    if (is_complete()) {
        // _ts should be unchanged, append it to pa outside lock.
        _last_active_real_ms = butil::gettimeofday_ms();
        mu.unlock();
        const int rc = pa->Write(_ts);
        if (rc == 0) {
            EventLogManager event_log_manager(EVENT_LOG_HLS, is_internal, pa->remote_side());
            event_log_manager.print_event_log(EVENT_LOG_START_PLAY, charge_key, _ts.size(), 0);
            event_log_manager.print_event_log(EVENT_LOG_STOP_PLAY, charge_key, _ts.size(), 0);
            VLOG(99) << "Write " << *this << " to pa@" << pa->remote_side();
        } else if (errno != brpc::EOVERCROWDED) {
            VPLOG(99) << "Fail to write " << *this << " to pa@"
                      << pa->remote_side();
        } else {
            LOG(WARNING) << "Fail to write " << *this << " to pa@"
                         << pa->remote_side()
                         << " temporarily, queue to FlushDownloadersThread";
            // Wrap the attachment into a downloader and queue it into the
            // flushing thread for periodic flushing.
            get_flush_downloaders_thread()->add_downloader(
                new TsDownloader(pa, _ts, is_internal, charge_key));
        }
    } else {
        const size_t old_ts_size = _ts.size();
        butil::intrusive_ptr<TsDownloader> d(
            new TsDownloader(pa, _ts, is_internal, charge_key));
        if (d->has_unsent()) {
            mu.unlock();
            if (d->flush() != 0) {  // permanent error (of the connection).
                return;
            }
            mu.lock();
            if (_ts.size() != old_ts_size) {
                // new TS data was appended.
                _ts.append_to(&d->_unsent, old_ts_size);
            }
        }
        _downloaders.push_back(d);
        mu.unlock();
        // remove_downloader_static() owns one ref of this and the downloader
        butil::intrusive_ptr<TsEntry>(this).detach();
        pa->NotifyOnStopped(brpc::NewCallback<TsEntry*, TsDownloader*>
                            (remove_downloader_static, this, d.detach()));
        VLOG(99) << "Write " << *this << " to pa@" << pa->remote_side()
                 << " and register the pa";
    }
}

void TsEntry::remove_downloader(const TsDownloader* downloader) {
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    for (size_t i = 0; i < _downloaders.size(); ++i) {
        if (_downloaders[i] == downloader) {
            _downloaders[i] = _downloaders.back();
            _downloaders.pop_back();
            mu.unlock();
            VLOG(99) << "Removed " << key() << '/' << seq_num()
                     << " downloader=" << downloader << " pa="
                     << downloader->_pa;
            return;
        }
    }
}

void TsEntry::remove_downloader_static(
    TsEntry* ts_entry_raw, TsDownloader* downloader_raw) {
    // note: don't add ref again, already added before NotifyOnStopped
    butil::intrusive_ptr<TsEntry> ts_entry(ts_entry_raw, false/*note*/);
    butil::intrusive_ptr<TsDownloader> downloader(downloader_raw, false/*note*/);
    if (downloader->_pa_removed) {
        // The downloader is already or about to be removed, no need to
        // remove it from global.
        return;
    }
    ts_entry->remove_downloader(downloader.get());
}

int TsEntry::append_ts_data(const butil::IOBuf& data, bool count_one_keyframe) {
    if (data.empty()) {
        LOG(ERROR) << "Param[data] is empty";
        return -1;
    }
    {
        std::unique_lock<pthread_mutex_t> mu(_mutex);
        if (is_complete()) {
            mu.unlock();
            LOG(FATAL) << "Append to already-completed " << *this;
            return -1;
        }
        if (!is_begined()) {
            mu.unlock();
            LOG(FATAL) << "Append to not-begined " << *this;
            return -1;
        }
        if (count_one_keyframe) {
            ++_nkf;
        }
        _ts.append(data);
        _tmp_downloaders = _downloaders;
    }
    size_t removal_count = 0;
    for (size_t i = 0; i < _tmp_downloaders.size(); ++i) {
        butil::intrusive_ptr<TsDownloader> & d = _tmp_downloaders[i];
        if (d->send(data) != 0) {
            d->_to_be_removed = true;
            ++removal_count;
        }
    }
    _tmp_downloaders.clear();
    if (removal_count) {
        {
            std::unique_lock<pthread_mutex_t> mu(_mutex);
            for (size_t i = 0; i < _downloaders.size();) {
                butil::intrusive_ptr<TsDownloader> & d = _downloaders[i];
                if (d->_to_be_removed) {
                    _tmp_downloaders.push_back(d);
                    _downloaders[i] = _downloaders.back();
                    _downloaders.pop_back();
                } else {
                    ++i;
                }
            }
        }
        for (size_t i = 0; i < _tmp_downloaders.size(); ++i) {
            _tmp_downloaders[i]->remove_pa();
        }
        _tmp_downloaders.clear();
    }
    return 0;
}

int TsEntry::begin_writing(int64_t start_timestamp, bool is_keyframe) {
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    if (is_complete()) {
        mu.unlock();
        LOG(FATAL) << "Begin already-completed " << *this;
        return -1;
    }
    if (!_ts.empty()) {
        mu.unlock();
        LOG(FATAL)<< "Begin TS with data, " << *this;
        return -1;
    }
    _start_timestamp = start_timestamp;
    _started_with_keyframe = is_keyframe;
    return 0;
}

void TsEntry::end_writing(int64_t end_timestamp) {
    if (end_timestamp < 0) {
        LOG(ERROR) << "Invalid end_timestamp=" << end_timestamp;
        return;
    }
    std::vector<butil::intrusive_ptr<TsDownloader> > downloaders;
    {
        std::unique_lock<pthread_mutex_t> mu(_mutex);
        _end_timestamp = end_timestamp;
        _last_active_real_ms = butil::gettimeofday_ms();
        // _downloaders will not be updated anymore
        downloaders.swap(_downloaders);
    }
    // Flush unsent buffers of all downloaders
    for (size_t i = 0; i < downloaders.size(); ++i) {
        butil::intrusive_ptr<TsDownloader> & d = downloaders[i];
        if (d->flush() != 0 || !d->has_unsent()) {
            // flushed or permanent error occurred.
            d->remove_pa();
        } else {
            // temporarily unflushable, queue the downloader to the flushing
            // thread for periodic flushing.
            get_flush_downloaders_thread()->add_downloader(d.get());
        }
    }
    VLOG(99) << "End " << *this;
}

bool TsEntry::is_idle_more_than(int64_t idle_ms, int64_t now_ms) const {
    return _last_active_real_ms >= 0 && _last_active_real_ms + idle_ms < now_ms;
}

std::ostream& operator<<(std::ostream& os, const TsEntry& ts) {
    os << "TS@" << ts.seq_num() << "(time=" << ts.start_timestamp() << '/';
    if (ts.is_complete()) {
        os << ts.end_timestamp() - ts.start_timestamp();
    } else {
        os << "incomplete";
    }
    pthread_mutex_lock(&ts._mutex);
    const size_t ts_size = ts.size();
    const size_t num_downloaders = ts._downloaders.size();
    pthread_mutex_unlock(&ts._mutex);
    os << " sz=" << ts_size
       << " nd=" << num_downloaders
       << " kf=" << ts.keyframe_count()
       << ')';
    return os;
}

/////////////// TsQueue ///////////////

TsQueue::TsQueue(const TsQueueKey& ts_queue_key)
    : brpc::RtmpStreamBase(false)
    , _key(ts_queue_key.key)
    , _first_avc_seq_header_delay(0)
    , _first_video_message_delay(0)
    , _first_aac_seq_header_delay(0)
    , _first_audio_message_delay(0)
    , _ts_writer(&_ts_data)
    , _first_seq_num(SEQUENCE_NUMBER_INIT)
    , _write_seq_num(SEQUENCE_NUMBER_INIT - 1)
    , _ts_duration_ms(ts_queue_key.ts_duration_ms)
    , _ts_num_per_m3u8(ts_queue_key.ts_num_per_m3u8)
    , _duetime_ms(make_duetime(butil::gettimeofday_ms(), _ts_duration_ms * get_stored_ts_num(_ts_num_per_m3u8)))
    , _nts_started_with_kf(0)
    , _ts_entry_list(malloc(sizeof(butil::intrusive_ptr<TsEntry>) * get_stored_ts_num(_ts_num_per_m3u8)), 
                            sizeof(butil::intrusive_ptr<TsEntry>) * get_stored_ts_num(_ts_num_per_m3u8),
                            butil::OWNS_STORAGE)
    , _on_idle_callback(NULL) 
    , _started_with_keyframe_in_ts(ts_queue_key.started_with_keyframe) {
    pthread_mutex_init(&_mutex, NULL);
    for (int i = 0; i < FLAGS_ts_pre_generated; ++i) {
        _ts_entry_list.push();
    }
}

TsQueue::~TsQueue() {
    pthread_mutex_destroy(&_mutex);
    VLOG(99) << __FUNCTION__ << '(' << this << "), play_key=" << _key;
    google::protobuf::Closure* cb =
        _on_idle_callback.exchange(NULL, butil::memory_order_relaxed);
    if (cb) {
        CHECK(false) << "TsQueue is destructed unexpectedly";
        cb->Run();
    }
}

void TsQueue::Describe(std::ostream& os,
                       const brpc::DescribeOptions&) const {
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    const int64_t saved_avcsh_delay = _first_avc_seq_header_delay;
    const int64_t saved_v_delay = _first_video_message_delay;
    const int64_t saved_aacsh_delay = _first_aac_seq_header_delay;
    const int64_t saved_a_delay = _first_audio_message_delay;
    const int64_t saved_duetime_ms = _duetime_ms;
    const int saved_nts = _nts_started_with_kf;
    const int64_t begin_seq_num = _first_seq_num;
    const int64_t end_seq_num = _write_seq_num + 1;
    mu.unlock();
    
    os << "vsh1=" << saved_avcsh_delay / 1000.0
       << "ms v1=" << saved_v_delay / 1000.0
       << "ms ash1=" << saved_aacsh_delay / 1000.0
       << "ms a1=" << saved_a_delay / 1000.0
       << "ms due=" << saved_duetime_ms - butil::gettimeofday_ms()
       << "ms k=" << saved_nts;
    for (int64_t i = begin_seq_num; i < end_seq_num; ++i) {
        const butil::intrusive_ptr<TsEntry>* ppentry = NULL;
        {
            std::unique_lock<pthread_mutex_t> mu2(_mutex);
            ppentry = _ts_entry_list.top(i - _first_seq_num);
        }
        if (ppentry == NULL) {
            os << " TS@" << i << "=null";
        } else {
            os << " " << **ppentry;
        }
    }
}

void TsQueue::remove_oldest_entry(butil::intrusive_ptr<TsEntry>* oldest_entry) {
    if (!oldest_entry) {
        return;
    }
    _ts_entry_list.pop(oldest_entry);
    CHECK_EQ((*oldest_entry)->seq_num(), _first_seq_num);
    ++_first_seq_num;
    if ((*oldest_entry)->is_started_with_keyframe()) {
        --_nts_started_with_kf;
    }
}

void TsQueue::write_next() {
    // Make deallocation of the oldest entry be outside lock.
    std::vector<butil::intrusive_ptr<TsEntry> > oldest_entries;
    const int64_t now_ms = butil::gettimeofday_ms();
    {
        std::unique_lock<pthread_mutex_t> mu(_mutex);
        if (_write_seq_num < SEQUENCE_NUMBER_INIT) {
            _write_seq_num = SEQUENCE_NUMBER_INIT;
        } else {
            ++_write_seq_num;
        }
        do {
            butil::intrusive_ptr<TsEntry> oldest_entry;
            butil::intrusive_ptr<TsEntry>* top_entry = _ts_entry_list.top();
            if (!_ts_entry_list.full() &&
                !(top_entry && (*top_entry) && 
                (*top_entry)->is_idle_more_than(_ts_duration_ms * get_stored_ts_num(_ts_num_per_m3u8), now_ms))) {
                break;
            }
            remove_oldest_entry(&oldest_entry);
            oldest_entries.push_back(oldest_entry);
        } while (true);
        _ts_entry_list.push();
    }
    oldest_entries.clear();
}

bool TsQueue::get_ts_entry(
    int64_t seq_num, butil::intrusive_ptr<TsEntry>* ptr) {
    if (seq_num < 0) {
        return false;
    }
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    butil::intrusive_ptr<TsEntry>* ppentry =
        _ts_entry_list.top(seq_num - _first_seq_num);
    if (ppentry == NULL) {
        return false;
    }
    if ((*ppentry) == NULL) {
        return false;
    }
    CHECK_EQ((*ppentry)->seq_num(), seq_num) << "Unmatched seq_num";
    *ptr = *ppentry;
    return true;
}

bool TsQueue::get_or_new_ts_entry(
    int64_t seq_num, butil::intrusive_ptr<TsEntry>* ptr) {
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    butil::intrusive_ptr<TsEntry>* ppentry =
        _ts_entry_list.top(seq_num - _first_seq_num);
    if (ppentry == NULL) {
        return false;
    }
    if ((*ppentry) == NULL) {
        (*ppentry).reset(new TsEntry(key(), seq_num));
        *ptr = *ppentry;
    } else {
        if ((*ppentry)->seq_num() != seq_num) {
            LOG(FATAL) << "Unmatched seq_num";
            return false;
        }
        *ptr = *ppentry;
    }
    return true;
}

int TsQueue::generate_media_playlist(std::ostream& os, int64_t* start_seq_num,
                                     const butil::StringPiece& stream_name,
                                     const butil::StringPiece& queries) {
    const int64_t duetime_ms = make_duetime(butil::gettimeofday_ms(), 
            _ts_duration_ms * get_stored_ts_num(_ts_num_per_m3u8));
    int64_t begin_seq_num = SEQUENCE_NUMBER_INIT - 1;
    int64_t end_seq_num = begin_seq_num;
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    if (_duetime_ms < 0) {  // disabled
        errno = EAGAIN;
        return -1;
    }
    _duetime_ms = duetime_ms;
    const int64_t saved_write_seq_num = _write_seq_num;
    const int64_t saved_first_seq_num = _first_seq_num;
    if (*start_seq_num < SEQUENCE_NUMBER_INIT) {
        int nts_started_with_kf = _nts_started_with_kf;
        mu.unlock();
        if (nts_started_with_kf > 0) {
            for (int64_t i = saved_write_seq_num; i >= saved_first_seq_num; --i) {
                butil::intrusive_ptr<TsEntry> ts_entry;
                if (!get_ts_entry(i, &ts_entry)) {
                    break;
                }
                if (ts_entry->is_started_with_keyframe()) {
                    if (nts_started_with_kf == 1 || ts_entry->is_complete()) {
                        begin_seq_num = i;
                        break;
                    }
                    --nts_started_with_kf;
                }
            }
        }
        if (begin_seq_num < SEQUENCE_NUMBER_INIT) {
            // Audio/Video data has not been received yet, just return an error.
            if (_write_seq_num < SEQUENCE_NUMBER_INIT) {
                return -1;
            }
            // Start playing from the first TS or the TS before the TS being 
            // written(probably incomplete).
            begin_seq_num = std::max(SEQUENCE_NUMBER_INIT, _write_seq_num - 1);
        }

        end_seq_num = begin_seq_num + std::min(FLAGS_ts_pre_generated + 1, _ts_num_per_m3u8);
        *start_seq_num = begin_seq_num;
    } else {
        mu.unlock();
        end_seq_num = saved_write_seq_num + std::min(FLAGS_ts_pre_generated + 1, _ts_num_per_m3u8 - 1);
        begin_seq_num = std::max(*start_seq_num,  end_seq_num - _ts_num_per_m3u8);
    }
    os << "#EXTM3U\n"
        "#EXT-X-VERSION:3\n"
        "#EXT-X-MEDIA-SEQUENCE:" << begin_seq_num
       << "\n#WRITE-SEQUENCE:" << saved_write_seq_num
       << "\n#EXT-X-TARGETDURATION:"
       << (long long)ceil(_ts_duration_ms / 1000.0) << "\n";
    // FIXME: all TS with sps/pps should be tagged with EXT-X-DISCONTINUITY
    if (begin_seq_num == *start_seq_num) {
        os << "#EXT-X-DISCONTINUITY\n";
    }
    for (int64_t i = begin_seq_num; i < end_seq_num; ++i) {
        os << "#EXTINF:" << _ts_duration_ms / 1000.0 << ",\n"
           << stream_name << ".seq" << i << ".ts";
        if (!queries.empty()) {
            os << '?' << queries;
        }
        os << '\n';
    }
    return 0;
}

int TsQueue::SendMetaData(const brpc::RtmpMetaData& metadata, const butil::StringPiece& name) {
    // if (_ts_writer.Write(metadata) != 0) {
    //     LOG(ERROR) << "Fail to write metadata into TS";
    //     return -1;
    // }
    // return flush();
    return 0;
}

int TsQueue::SendAudioMessage(const brpc::RtmpAudioMessage& msg) {
    if (msg.IsAACSequenceHeader()) {
        if (!_first_aac_seq_header_delay) {
            _first_aac_seq_header_delay = butil::gettimeofday_us() - create_realtime_us();
        }
        const size_t old_size = _ts_data.size();
        butil::Status st = _ts_writer.Write(msg);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to convert sequence header to TS@" << key()
                         << ", " << st;
            return -1;
        }
        CHECK_EQ(old_size, _ts_data.size())
            << "_ts_data is changed after writing a sequence header";
        return 0;
    } else {
        if (!_first_audio_message_delay) {
            _first_audio_message_delay = butil::gettimeofday_us() - create_realtime_us();
        }
    }
    butil::intrusive_ptr<TsEntry> ts_entry;
    if (get_or_move_to_next_ts_entry(&ts_entry, msg.timestamp, false) != 0) {
        LOG(ERROR) << "Fail to get_or_move_to_next_ts_entry";
        return -1;
    }
    butil::Status st = _ts_writer.Write(msg);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to convert audio message to TS@" << key()
                   << ", " << st;
        return -1;
    }
    if (_ts_data.empty()) {
        return 0;
    }
    if (ts_entry->append_ts_data(_ts_data, false) != 0) {
        LOG(WARNING) << "Fail to append to TS@" << key();
        return -1;
    }
    _ts_data.clear();
    return 0;
}
    
int TsQueue::SendVideoMessage(const brpc::RtmpVideoMessage& msg) {
    bool is_keyframe = false;
    if (msg.IsAVCSequenceHeader()) {
        if (!_first_avc_seq_header_delay) {
            _first_avc_seq_header_delay = butil::gettimeofday_us() - create_realtime_us();
        }
        const size_t old_size = _ts_data.size();
        butil::Status st = _ts_writer.Write(msg);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to convert sequence header to TS@" << key()
                         << ", " << st;
            return -1;
        }
        CHECK_EQ(old_size, _ts_data.size())
            << "_ts_data is changed after writing a sequence header";
        return 0;
    } else {
        if (msg.frame_type == brpc::FLV_VIDEO_FRAME_KEYFRAME) {
            is_keyframe = true;
        }
        if (!_first_video_message_delay) {
            _first_video_message_delay = butil::gettimeofday_us() - create_realtime_us();
        }
    }
    butil::intrusive_ptr<TsEntry> ts_entry;
    if (get_or_move_to_next_ts_entry(&ts_entry, msg.timestamp, is_keyframe) != 0) {
        LOG(ERROR) << "Fail to get_or_move_to_next_ts_entry";
        return -1;
    }
    butil::Status st = _ts_writer.Write(msg);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to convert video message to TS@" << key()
                   << ", " << st;
        return -1;
    }
    if (_ts_data.empty()) {
        return 0;
    }
    if (ts_entry->append_ts_data(_ts_data, is_keyframe) != 0) {
        LOG(WARNING) << "Fail to append to TS@" << key();
        return -1;
    }
    _ts_data.clear();
    return 0;
}

// TsQueue is associated to many connections, not one.
butil::EndPoint TsQueue::remote_side() const { return butil::EndPoint(); }
butil::EndPoint TsQueue::local_side() const { return butil::EndPoint(); }

int TsQueue::get_or_move_to_next_ts_entry(
    butil::intrusive_ptr<TsEntry>* out, int64_t timestamp, bool is_keyframe) {
    butil::intrusive_ptr<TsEntry> ts_entry;
    bool need_move_next = false;
    do {
        if (_write_seq_num < SEQUENCE_NUMBER_INIT) { // first write
            need_move_next = true;
            break;
        }
        if (!get_ts_entry(_write_seq_num, &ts_entry)) {
            LOG(ERROR) << "Fail to get TS@" << _write_seq_num;
            return -1;
        }
        // TODO(zhujiashun): refine this code
        if (_started_with_keyframe_in_ts) {
            if (timestamp >= ts_entry->start_timestamp() + _ts_duration_ms) {
                if (is_keyframe) {
                    need_move_next = true;
                }
                break;
            }
            if (!is_keyframe) {
                break;
            }
            if (!ts_entry->is_started_with_keyframe()) {
                // if pusher pushes non-keyframe first(by purpose or by mistake), then we
                // need to break at this moment and let next ts be started with keyframe.
                need_move_next = true;
                break;
            }
        } else {
            if (timestamp >= ts_entry->start_timestamp() + _ts_duration_ms) {
                // The TS is long enough.
                need_move_next = true;
                break;
            }
            if (!is_keyframe) {
                // The TS is not enough and the new message is not keyframe,
                // keep writing.
                break;
            }
            if (!ts_entry->is_started_with_keyframe()) {
                // break at keyframe if the previous TS is not started with keyframe.
                need_move_next = true;
                break;
            }
        }

        // Even if the above ts_entry->is_started_with_keyframe() is true, the keyframe
        // count can also be zero since the addition and the mark of _started_with_keyframe
        // is not atomic.
        if (ts_entry->keyframe_count() == 0) {
            need_move_next = true;
            break;
        }

        // The previous TS already has at least one keyframes (and started with
        // keyframe), calculate average duration of keyframes to see if the next
        // GOP(probably the same duration) can be fit in the entry. This code
        // combines "small" GOPs together to prevent clients from downloading
        // small TS constantly and pay 1-RTT cost for each TS. Explain: most
        // clients as we observed do not start downloading TS before the
        // previous TS gets downloaded, thus there's a 1-RTT stall between
        // consecutive TS.
        const int64_t avg_duration_ms =
            (timestamp - ts_entry->start_timestamp()) / ts_entry->keyframe_count();
        if (timestamp + avg_duration_ms >
            ts_entry->start_timestamp() + _ts_duration_ms) {
            need_move_next = true;
            break;
        }
    } while (0);
    if (need_move_next) {
        if (ts_entry) {
            ts_entry->end_writing(timestamp);
        }
        write_next();
        _ts_writer.add_pat_pmt_on_next_write();
        if (!get_or_new_ts_entry(_write_seq_num, &ts_entry)) {
            LOG(ERROR) << "Fail to get TsEntry@" << _write_seq_num;
            return -1;
        }
        if (ts_entry->begin_writing(timestamp, is_keyframe) != 0) {
            LOG(ERROR) << "Fail to begin writing " << *ts_entry;
            return -1;
        }
        if (ts_entry->is_started_with_keyframe()) {
            ++_nts_started_with_kf;
        }
        VLOG(99) << "Move to TS@" << _write_seq_num << "(kf="
                 << is_keyframe << ')';
    }
    out->swap(ts_entry);
    return 0;
}

bool get_ts_queue(const TsQueueKey& key,
                  butil::intrusive_ptr<TsQueue>* ptr) {
    return get_ts_queue_map()->get(key, ptr);
}
static pthread_once_t g_remove_idle_queue_thread_once = PTHREAD_ONCE_INIT;
void create_remove_idle_queue_thread() {
    bthread_t th;
    CHECK_EQ(0, bthread_start_background(
                 &th, NULL, TsQueue::remove_idle_queue_thread, NULL));
}
bool get_or_new_ts_queue(const TsQueueKey& key,
                         butil::intrusive_ptr<TsQueue>* ptr) {
    pthread_once(&g_remove_idle_queue_thread_once,
                 create_remove_idle_queue_thread);
    return get_ts_queue_map()->get_or_new(key, ptr);
}

void TsQueue::on_idle(google::protobuf::Closure* callback) {
    google::protobuf::Closure* prev_cb =
        _on_idle_callback.exchange(callback, butil::memory_order_relaxed);
    if (prev_cb != NULL) {
        LOG(FATAL) << "on_idle of TsQueue on key=" << key() << " is alreay"
            " called before, replace and run the old callback";
        prev_cb->Run();
        return;
    }
}

static size_t get_ts_queue_count(void*) {
    TsQueueMap* m = butil::has_leaky_singleton<TsQueueMap>();
    return m != NULL ? m->size() : 0;
}
struct IsTsQueueIdle {
    IsTsQueueIdle() : _now_ms(butil::gettimeofday_ms()) {}
    bool operator()(TsQueue& val) const {
        return val.disable_on_idle(_now_ms);
    }
private:
    int64_t _now_ms;
};
void* TsQueue::remove_idle_queue_thread(void*) {
    bvar::PassiveStatus<size_t> ts_queue_count(
        "hls_stream_count", get_ts_queue_count, NULL);
    std::vector<std::pair<TsQueueKey, butil::intrusive_ptr<TsQueue> > > removals;
    TsQueueMap* m = get_ts_queue_map();
    int64_t last_release_mem_ms = butil::gettimeofday_ms();
    while (true) {
        IsTsQueueIdle filter;
        m->remove_by(filter, &removals);
        for (size_t i = 0; i < removals.size(); ++i) {
            butil::intrusive_ptr<TsQueue> & tsq = removals[i].second;
            google::protobuf::Closure* cb =
                tsq->_on_idle_callback.exchange(NULL, butil::memory_order_relaxed);
            if (cb) {
                cb->Run();
            }
            VLOG(99) << "Removed idle TsQueue on key=" << removals[i].first.key;
        }
        removals.clear();
        const int64_t now_ms = butil::gettimeofday_ms();
        if (now_ms > last_release_mem_ms + 5000/*5s*/) {
            last_release_mem_ms = now_ms;
            MallocExtension::instance()->ReleaseFreeMemory();
        }
        bthread_usleep(1000000/*1s*/);
    }
    return NULL;
}

bool TsQueue::disable_on_idle(int64_t now_ms) {
    // one ref is stored in global map of TsQueue, another ref is stored in
    // the PlayerGroup
    if (ref_count() <= 2/*note*/ && _duetime_ms >= 0 && _duetime_ms < now_ms) {
        std::unique_lock<pthread_mutex_t> mu(_mutex);
        if (ref_count() <= 2 && _duetime_ms >= 0 && _duetime_ms < now_ms) {
            _duetime_ms = -1;
            return true;
        }
    }
    return false;
}

int TsQueue::SendStopMessage(const butil::StringPiece& error_desc) {
    {
        std::unique_lock<pthread_mutex_t> mu(_mutex);
        if (_duetime_ms < 0) {
            return 0;
        }
        _duetime_ms = -1;
    }
    get_ts_queue_map()->remove(TsQueueKey{_key, _started_with_keyframe_in_ts, _ts_duration_ms, 
                                          _ts_num_per_m3u8});
    google::protobuf::Closure* cb =
        _on_idle_callback.exchange(NULL, butil::memory_order_relaxed);
    if (cb) {
        cb->Run();
    }
    return 0;
}
