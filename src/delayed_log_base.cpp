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

#include "butil/memory/singleton_on_pthread_once.h"
#include "delayed_log_base.h"

static const int GRAB_DELAYED_LOG_INTERVAL_MS = 50;

DelayedLogBase::~DelayedLogBase() {}

// Combine two circular linked list into one.
struct CombineDelayedLogBase {
    void operator()(DelayedLogBase* & s1, DelayedLogBase* s2) const {
        if (s2 == NULL) {
            return;
        }
        if (s1 == NULL) {
            s1 = s2;
            return;
        }
        s1->InsertBeforeAsList(s2);
    }
};

// A thread and a special bvar to collect samples submitted.
class LogPrinter : public bvar::Reducer<DelayedLogBase*, CombineDelayedLogBase> {
public:
    LogPrinter();
    ~LogPrinter();
    
private:
    // The thread for collecting TLS submissions.
    void grab_thread();

    static void* run_grab_thread(void* arg) {
        static_cast<LogPrinter*>(arg)->grab_thread();
        return NULL;
    }

private:
    bool _created;              // Mark validness of _grab_thread.
    bool _stop;                 // Set to true in dtor.
    pthread_t _grab_thread;     // For joining.
};

LogPrinter::LogPrinter()
    : _created(false)
    , _stop(false)
    , _grab_thread(0) {
    int rc = pthread_create(&_grab_thread, NULL, run_grab_thread, this);
    if (rc != 0) {
        LOG(ERROR) << "Fail to create LogPrinter, " << berror(rc);
    } else {
        _created = true;
    }
}

LogPrinter::~LogPrinter() {
    if (_created) {
        _stop = true;
        pthread_join(_grab_thread, NULL);
        _created = false;
    }
}

void LogPrinter::grab_thread() {
    int64_t last_wakeup_time = 0;
    while (!_stop) {
        // Collect TLS submissions and give them to dump_thread.
        butil::LinkNode<DelayedLogBase>* head = this->reset();
        if (head) {
            butil::LinkNode<DelayedLogBase> tmp_root;
            head->InsertBeforeAsList(&tmp_root);
            head = NULL;
            
            // Group samples by preprocessors.
            for (butil::LinkNode<DelayedLogBase>* p = tmp_root.next(); p != &tmp_root;) {
                butil::LinkNode<DelayedLogBase>* saved_next = p->next();
                p->RemoveFromList();
                p->value()->print_and_destroy();
                p = saved_next;
            }
        }
        const int64_t sleep_ms = last_wakeup_time
            + GRAB_DELAYED_LOG_INTERVAL_MS - butil::gettimeofday_ms();
        if (sleep_ms > 0) {
            usleep(sleep_ms * 1000L);
        }
        last_wakeup_time = butil::gettimeofday_ms();
    }
}

void DelayedLogBase::submit() {
    *butil::get_leaky_singleton<LogPrinter>() << this;
}
