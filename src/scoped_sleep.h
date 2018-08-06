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

#ifndef MEDIA_SERVER_SCOPED_SLEEP_H
#define MEDIA_SERVER_SCOPED_SLEEP_H

#include "bthread/bthread.h"
#include "butil/time.h"

class ScopedSleep {
public:
    ScopedSleep(int64_t sleep_time_us)
        : _sleep_time_us(sleep_time_us) 
        , _base_us(butil::gettimeofday_us()) {} 

    ~ScopedSleep() {
        int64_t now_us = butil::gettimeofday_us();
        int64_t passed_time_us = now_us - _base_us;
        int64_t sleep_time_us = _sleep_time_us - passed_time_us;

        if (sleep_time_us > 0) {
            bthread_usleep(sleep_time_us);
        }   
    }
private:
    int64_t _sleep_time_us;
    int64_t _base_us;
};

#endif // MEDIA_SERVER_SCOPED_SLEEP_H
