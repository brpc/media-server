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

#ifndef MEDIA_SERVER_EVENT_LOG_MANAGER_H
#define MEDIA_SERVER_EVENT_LOG_MANAGER_H

#include "butil/endpoint.h"
#include "delayed_log_base.h"

enum EventLogMediaType {
    EVENT_LOG_RTMP,
    EVENT_LOG_FLV,
    EVENT_LOG_HLS,
};

enum EventLogAction {
    EVENT_LOG_START_PLAY,
    EVENT_LOG_STOP_PLAY,
    EVENT_LOG_START_PUBLISH,
    EVENT_LOG_STOP_PUBLISH,
};

struct RequestId {
    uint64_t data[2];
};
std::ostream& operator<<(std::ostream& os, const RequestId&);

class EventLogManager {
public:
    EventLogManager(EventLogMediaType type, bool internal_service, butil::EndPoint end_point);
    ~EventLogManager();
    void print_event_log(EventLogAction action, const std::string& key,
                         size_t sent_bytes, size_t received_bytes);
    void reset_remote_side(const butil::EndPoint& end_point);
private:
    struct EventLog;
    const EventLogMediaType _type;
    const bool _internal_service;
    bool _is_play;
    bool _started;
    bool _ever_started;
    int64_t _last_print_event_log_time;
    size_t _sent_bytes_base;
    size_t _received_bytes_base;
    RequestId _request_id;
    butil::EndPoint _end_point;
};

#endif // MEDIA_SERVER_EVENT_LOG_MANAGER_H
