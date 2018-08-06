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

#include <gflags/gflags_declare.h>
#include "butil/atomicops.h"
#include "butil/third_party/murmurhash3/murmurhash3.h"
#include "event_log_manager.h"
#include "rtmp_forward_service.h"

DECLARE_int32(dump_stats_interval);
DECLARE_int32(port);

static RequestId EMPTY_REQUEST_ID = {{0}};

static butil::subtle::Atomic64 g_request_id_counter = 1;
void generate_request_id(RequestId* request_id) {
    // getpid() took 400-500ns, cache it.
    static const uint64_t pid = getpid();
    const uint64_t seq =
        butil::subtle::NoBarrier_AtomicIncrement(&g_request_id_counter, 1);
    // Not use snprintf which is (relatively) slow.
    char buf[64];
    char* p = buf;
    *(int*)p = butil::ip2int(butil::my_ip());
    p += sizeof(int);
    *(int32_t*)p = FLAGS_port;
    p += 4;
    *(uint64_t*)p = pid;
    p += 8;
    *(uint64_t*)p = seq;
    p += 8;
    *(int64_t*)p = butil::gettimeofday_us();
    p += 8;
    butil::MurmurHash3_x64_128(buf, p - buf, 0, request_id->data);
}
        
static char REQUEST_ID_ALPHABETS[] =
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_";
BAIDU_CASSERT(sizeof(REQUEST_ID_ALPHABETS) == 65,
              sizeof_request_id_alphabets_must_be_65);

std::ostream& operator<<(std::ostream& os, const RequestId& id) {
    for (int i = 0; i < 2; ++i) {
        uint64_t d = id.data[i];
        for (int j = 0; j < 10; ++j) {
            os << REQUEST_ID_ALPHABETS[d & 0x3F];
            d = (d >> 6);
        }
        os << REQUEST_ID_ALPHABETS[d & 0x3F];
    }
    return os;
}

enum EventLogOperation {
    EVENT_LOG_OPERATION_START,
    EVENT_LOG_OPERATION_STOP,
    EVENT_LOG_OPERATION_CONTINUE,
};

struct EventLogManager::EventLog : public DelayedLogBase {
    EventLogMediaType type;
    EventLogOperation ops;
    bool internal_service;
    bool is_play;
    std::string key;
    RequestId request_id;
    int64_t realtime;
    size_t sent_bytes;
    size_t received_bytes;
    butil::EndPoint end_point;

    void print_and_destroy() {
        print_to(LOG_STREAM(INFO));
        delete this;
    }

private:
    void print_to(std::ostream&);
};

// static void print_event_time(std::ostream& os, int64_t microseconds) {
//     time_t t = microseconds / 1000000L;
//     struct tm local_tm = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL};
// #if _MSC_VER >= 1400
//     localtime_s(&local_tm, &t);
// #else
//     localtime_r(&t, &local_tm);
// #endif
//     char buf[64];
//     int len = snprintf(buf, sizeof(buf),
//                        "%04d%02d%02d-%02d:%02d:%02d.%06d",
//                        local_tm.tm_year + 1900,
//                        local_tm.tm_mon + 1,
//                        local_tm.tm_mday,
//                        local_tm.tm_hour,
//                        local_tm.tm_min,
//                        local_tm.tm_sec,
//                        (int)(microseconds - t * 1000000L));
//     os << butil::StringPiece(buf, len);
// }

void EventLogManager::EventLog::print_to(std::ostream& os) {
    // Current precision of stats is 1min, the difference between the time
    // of submission and printing log is not important. Thus we don't
    // print the submission time right now.
    //print_event_time(os, realtime);
    //os << ' ';
    switch (ops) {
    case EVENT_LOG_OPERATION_START:
        os << "start_";
        break;
    case EVENT_LOG_OPERATION_STOP:
        os << "stop_";
        break;
    case EVENT_LOG_OPERATION_CONTINUE:
        os << "on_";
        break;
    }
    switch (type) {
    case EVENT_LOG_RTMP:
        break;
    case EVENT_LOG_FLV:
        os << "flv_";
        break;
    case EVENT_LOG_HLS:
        os << "hls_";
        break;
    }
    butil::StringPiece tmpkey = key;
    if (!is_play && tmpkey.ends_with(PUBLISH_PROXY_SUFFIX)) {
        tmpkey.remove_suffix(strlen(PUBLISH_PROXY_SUFFIX));
    }
    // Distinguish requests from cdn nodes or clients
    os << (is_play ? "play" : "publish")
       << " key:" << tmpkey
       << " request:" << request_id
       << " send:" << sent_bytes
       << " recv:" << received_bytes
       << " type:" << (internal_service ? "internal" : "external")
       << " endpoint:" << end_point;
}

EventLogManager::EventLogManager(EventLogMediaType type, bool internal_service, butil::EndPoint end_point)
    : _type(type)
    , _internal_service(internal_service)
    , _is_play(true)
    , _started(false)
    , _ever_started(false)
    , _last_print_event_log_time(0)
    , _sent_bytes_base(0)
    , _received_bytes_base(0)
    , _request_id(EMPTY_REQUEST_ID)
    , _end_point(end_point) {
}

EventLogManager::~EventLogManager() {
    LOG_IF(FATAL, _started) << "EventLogManager destructs before "
        "print_event_log() being called with a stop action";
}

void EventLogManager::reset_remote_side(const butil::EndPoint& end_point) {
    _end_point = end_point;
}

void EventLogManager::print_event_log(
    EventLogAction action, const std::string& key,
    size_t sent_bytes, size_t received_bytes) {
    if (FLAGS_dump_stats_interval <= 0) { // feature disabled
        return;
    }
    const int64_t now = butil::gettimeofday_us();
    EventLogOperation ops;
    bool update_base = false;
    if (action == EVENT_LOG_START_PLAY || action == EVENT_LOG_START_PUBLISH) {
        _is_play = (action == EVENT_LOG_START_PLAY);
        _ever_started = true;
        if (_started) {
            if (now < _last_print_event_log_time +
                FLAGS_dump_stats_interval * 1000000L) {
                return;
            }
            ops = EVENT_LOG_OPERATION_CONTINUE;
        } else {
            generate_request_id(&_request_id);
            _started = true;
            ops = EVENT_LOG_OPERATION_START;
        }
    } else if (action == EVENT_LOG_STOP_PLAY ||
               action == EVENT_LOG_STOP_PUBLISH) {
        const bool is_play = (action == EVENT_LOG_STOP_PLAY);
        if (is_play != _is_play) {
            LOG(ERROR) << "ERROR: started by start_"
                       << (_is_play ? "play" : "publish")
                       << ", but stopped by stop_"
                       << (is_play ? "play" : "publish");
            return;
        }
        if (!_ever_started) {
            // no started ever, no need to print stop event.
            return;
        }
        if (!_started) {
            // already stopped, no need to print the stop event again.
            LOG_IF(ERROR, sent_bytes != _sent_bytes_base)
                << "sent_bytes was changed from " << _sent_bytes_base << " to "
                << sent_bytes << " after stop_play event";
            LOG_IF(ERROR, received_bytes != _received_bytes_base)
                << "received_bytes was changed from " << _received_bytes_base
                << " to " << received_bytes << " after stop_play event";
            return;
        }
        update_base = true;
        _started = false;
        ops = EVENT_LOG_OPERATION_STOP;
    } else {
        LOG(FATAL) << "Unknown action=" << action;
        return;
    }
    _last_print_event_log_time = now;

    EventLog* log = new EventLog;
    log->type = _type;
    log->ops = ops;
    log->internal_service = _internal_service;
    log->is_play = _is_play;
    log->key = key; 
    log->request_id = _request_id;
    log->realtime = now;
    log->sent_bytes = sent_bytes - _sent_bytes_base;
    log->received_bytes = received_bytes - _received_bytes_base;
    log->end_point = _end_point;
    log->submit();
    if (update_base) {
        _sent_bytes_base = sent_bytes;
        _received_bytes_base = received_bytes;
    }
}
