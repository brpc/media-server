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
#include "brpc/reloadable_flags.h"
#include "frame_queue.h"

DEFINE_int32(max_cached_keyframes, 1, "Max number of keyframes in GOP cache");
DEFINE_int32(max_cached_frames, 32768, "Max number of frames in GOP cache");
DEFINE_int32(max_jitter_ms, 5000, "The diff threshold beyond which frame is considered as jitter");
DEFINE_int32(frame_interval_ms, 40, "The default interval added to frame if jitter happens");

DEFINE_int32(min_cached_frames_without_keyframes, 128,
             "Min #frames in GOP cache without any keyframes");
// deprecated: (by default) works unless the keyframe is the only keyframe that received
DEFINE_int32(min_buffering_ms, 0, "Start playing from a keyframe that is received"
             " for at least so many milliseonds");

Frame::Frame()
    : type(FRAME_UNKNOWN)
    , timestamp(0)
    , corrected_time(0)
    , index(0) {
}

Frame::Frame(brpc::RtmpVideoMessage& msg)
    : type(FRAME_VIDEO)
    , timestamp(msg.timestamp)
    , corrected_time(0)
    , index(0)
    , frame_type(msg.frame_type)
    , vcodec(msg.codec) {
    data.swap(msg.data);
}

Frame::Frame(brpc::RtmpAudioMessage& msg)
    : type(FRAME_AUDIO)
    , timestamp(msg.timestamp)
    , corrected_time(0)
    , index(0)
    , acodec(msg.codec)
    , rate(msg.rate)
    , bits(msg.bits)
    , sound_type(msg.type) {
    data.swap(msg.data);
}

bool Frame::swap_as(brpc::RtmpVideoMessage& msg) {
    if (type != FRAME_VIDEO) {
        return false;
    }
    msg.timestamp = timestamp;
    msg.frame_type = frame_type;
    msg.codec = vcodec;
    data.swap(msg.data);
    return true;
}

bool Frame::swap_as(brpc::RtmpAudioMessage& msg) {
    if (type != FRAME_AUDIO) {
        return false;
    }
    msg.timestamp = timestamp;
    msg.codec = acodec;
    msg.rate = rate;
    msg.bits = bits;
    msg.type = sound_type;
    data.swap(msg.data);
    return true;
}

bool Frame::is_keyframe() const {
    return (type == FRAME_VIDEO || type == FRAME_USER_DATA) &&
        frame_type == brpc::FLV_VIDEO_FRAME_KEYFRAME;
}

FrameIndicator::FrameIndicator()
    : type(FrameIndicator::BY_CONFIG) {
}

FrameQueue::FrameQueue()
    : _max_index(INDEX_INIT + 1)
    , _version(VERSION_INIT)
    , _last_push_realtime_us(0)
    , _last_reset_realtime_us(butil::gettimeofday_us())
    , _last_msg_timestamp(0)
    , _last_msg_corrected_time(0)
    , _allowed_pusher(NULL) {
    pthread_mutex_init(&_mutex, NULL);
}

FrameQueue::~FrameQueue() {
    pthread_mutex_destroy(&_mutex);
}

bool FrameQueue::push(Frame& frame, const void* pusher) {
    const int64_t now_us = butil::gettimeofday_us();
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    if (pusher != _allowed_pusher) {
        return false;
    }
    int64_t delta = frame.timestamp - _last_msg_timestamp;
    if (delta > FLAGS_max_jitter_ms ||
        delta < 0 /* time back-off happens */) {
        // jitter happens
        delta = FLAGS_frame_interval_ms;
    }
    frame.corrected_time = _last_msg_corrected_time + delta;
    ++_max_index;
    frame.index = _max_index;
    _last_push_realtime_us = now_us;
    _last_msg_timestamp = frame.timestamp;
    _last_msg_corrected_time = frame.corrected_time;
    _frames.push_back(frame);
    if (frame.is_keyframe()) {
        _keyframes.push_back(&_frames.back());
    }

    // Access the gflags for only once to avoid reading different values
    // (may be reloaded).    
    const size_t saved_max_cached_frames = FLAGS_max_cached_frames;
    if (_frames.size() > saved_max_cached_frames) {
        // Too many frames, even if the oldest one is keyframe, we have to
        // remove it otherwise space may be bloated. This is a critical
        // situation that shouldn't happen in production environment.
        Frame & oldest = _frames.front();
        if (oldest.is_keyframe()) {
            CHECK_EQ(&oldest, _keyframes.front())
                << "_keyframes is inconsistent with _frames";
            _keyframes.pop_front();
        }
        _frames.pop_front();
        mu.unlock();
        LOG_EVERY_SECOND(ERROR)
            << "FrameQueue(" << _name << ") cached more than --max_cached_frames="
            << saved_max_cached_frames;
        return true;
    }
    
    // Remove oldest frames. Notice that we remove 2 frames at most so that
    // gop size keeps shrinking (size decreases for 1 in each push) when
    // there're many non-keyframes before the oldest keyframe.
    const size_t saved_max_cached_keyframes =
        std::max(FLAGS_max_cached_keyframes, 1);
    for (int i = 0; i < 2 && !_frames.empty(); ++i) {
        Frame & oldest = _frames.front();
        if (oldest.is_keyframe()) {
            int64_t cache_length = FLAGS_min_buffering_ms;
            if (_user_config != NULL) {
                cache_length = _user_config->get_cache_length();
            }
            cache_length = std::max(cache_length, (int64_t)0);
            if (_keyframes.size() <= saved_max_cached_keyframes ||
                // the second oldest keyframe is still later than
                // cache_length before
                _keyframes[1/*safe*/]->corrected_time + cache_length
                >= _last_msg_corrected_time ||
                (_user_config && !_user_config->has_enough_frame(_frames.size()))) {
                break;
            }
            CHECK_EQ(&oldest, _keyframes.front())
                << "_keyframes is inconsistent with _frames";
            _keyframes.pop_front();
            _frames.pop_front();
        } else if (!_keyframes.empty() ||
                   _frames.size() > (size_t)FLAGS_min_cached_frames_without_keyframes) {
            // If there exists keyframes, oldest non-keyframe is always
            // removeable. Otherwise keep minimum frames in audio-only
            // GOP cache.
            _frames.pop_front();
        }
    }
    return true;
}

bool FrameQueue::next(Frame* frame_out, size_t* index, size_t* version,
                      const FrameIndicator& indicator) {
    int64_t min_cache_length = -1;
    int64_t max_cache_length = -1;
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    if (version) {
        *version = _version;
    }
    if (_frames.empty()) {
        return false;
    }
    switch (indicator.type) {
    case FrameIndicator::BY_USING_LATEST:
        if (*index == INDEX_INIT || _max_index - *index > _frames.size()) {
            if (_frames.size() > 0) {
                *index = _max_index; 
                *frame_out = _frames.back();
                return true;
            }
            return false;
        }
        break;
    case FrameIndicator::BY_CONFIG:
        if (min_cache_length < 0 && max_cache_length < 0) {
            min_cache_length = FLAGS_min_buffering_ms;
            max_cache_length = -1;
        }
        break;
    case FrameIndicator::BY_PLAY_BACK:
        min_cache_length = (_user_config ?
                            _user_config->get_play_back_time_s(indicator.play_back_time_s) :
                            0) * 1000L;
        break;
    case FrameIndicator::BY_FIRST_FRAME_COUNT:
        // Calculate min_cache_length according to first frame counts
        // equation: (timestamp of the newest frame - timestamp of the oldest frame) / size of frame queue
        //           = min_cache_length / first frame counts
        min_cache_length = 0;
        if (_frames.size() >= 2) {
            int64_t frame_count = (_user_config ?
                    _user_config->get_first_frame_count(indicator.first_frame_count) : 0);
            min_cache_length = (_frames.back().timestamp - _frames.front().timestamp) *
            1000L * frame_count / _frames.size();
        } 
        break;
    }
    const size_t min_index = _max_index - _frames.size() + 1;
    if (*index + 1 >= min_index) {
        const size_t offset = *index + 1 - min_index;
        if (offset >= _frames.size()) {
            if (*index > _max_index) { // reset
                *index = INDEX_INIT;
            }
            // all frames inside queue are older, nothing to return.
            return false;
        }
        // The frame denoted by *index+1 exists.
        ++*index;
        *frame_out = _frames[offset];
        return true;
    }
    if (!_keyframes.empty()) {
        if (max_cache_length < 0 && min_cache_length >= 0) {
            // Find the latest keyframe regarding min_cache_length
            for (int64_t i = _keyframes.size() - 1; i >= 0; --i) {
                const Frame* frame = _keyframes[i];
                // the only keyframe left, don't check buffering time
                if (i == 0 ||
                    // the keyframe is buffered for enough time. There're
                    // probably enough frames after the keyframe to be sent
                    // to the client in-time.
                    frame->corrected_time + min_cache_length
                    <= _last_msg_corrected_time) {
                    *index = frame->index;
                    *frame_out = *frame;
                    return true;
                }
            }
            mu.unlock();
            CHECK(false) << "Impossible";
            return false;
        } else if (min_cache_length < 0 && max_cache_length >= 0) {
            // Find the latest keyframe regarding max_cache_length
            for (size_t i = 0; i <= _keyframes.size() - 1; ++i) {
                const Frame* frame = _keyframes[i];
                // the only keyframe left, don't check buffering time
                if (i == _keyframes.size() - 1 ||
                    frame->corrected_time + max_cache_length
                    >= _last_msg_corrected_time) {
                    *index = frame->index;
                    *frame_out = *frame;
                    return true;
                }
            }
            mu.unlock();
            CHECK(false) << "Impossible";
            return false;
        } else {
            LOG(ERROR) << "Fail to recognize max_cache_length=" << max_cache_length 
                       << ", min_cache_length=" << min_cache_length ;
            return false;
        }
    } else {
        // keyframe is absent, likely to be audio-only stream, just return the
        // newest frame regarding cache_length
        if (max_cache_length < 0 && min_cache_length >= 0) {
            for (size_t i = 0; i < _frames.size(); ++i) {
                const Frame& frame = _frames[_frames.size() - 1 - i];
                if (frame.corrected_time + min_cache_length
                    <= _last_msg_corrected_time) {
                    *index = _max_index - i;
                    *frame_out = frame;
                    return true;
                }
            }
            *index = _max_index;
            *frame_out = _frames.back();
            return true;
        } else if (min_cache_length < 0 && max_cache_length >= 0) {
            for (size_t i = 0; i < _frames.size(); ++i) {
                const Frame& frame = _frames[i];
                if (frame.corrected_time + max_cache_length
                    >= _last_msg_corrected_time) {
                    *index = _max_index - i;
                    *frame_out = frame;
                    return true;
                }
            }
            *index = _max_index;
            *frame_out = _frames.back();
            return true;
        } else {
            LOG(ERROR) << "Fail to recognize max_cache_length=" << max_cache_length 
                       << ", min_cache_length=" << min_cache_length ;
            return false;
        }
    }
}

void FrameQueue::reset(const void* allowed_pusher) {
    std::deque<Frame> saved_frames;
    std::deque<const Frame*> saved_keyframes;
    {
        std::unique_lock<pthread_mutex_t> mu(_mutex);
        _max_index = INDEX_INIT + 1;
        ++_version;
        _last_reset_realtime_us = butil::gettimeofday_us();
        _last_msg_timestamp = 0;
        _last_msg_corrected_time = 0;
        _allowed_pusher = allowed_pusher;
        // Swap out the container to make the deallocations outside lock.
        _frames.swap(saved_frames);
        _keyframes.swap(saved_keyframes);
    }
}

void FrameQueue::get_stats(Stats* s) {
    std::unique_lock<pthread_mutex_t> mu(_mutex);
    s->max_index = _max_index;
    s->last_push_realtime_us = _last_push_realtime_us;
    s->last_reset_realtime_us = _last_reset_realtime_us;
    s->nframes = _frames.size();
    s->nkeyframes = _keyframes.size();
    s->last_msg_timestamp = _last_msg_timestamp;
}

DefaultCursor::DefaultCursor(FrameQueue* q, const FrameIndicator& indicator)
    : _q(q)
    , _last_index(FrameQueue::INDEX_INIT)
    , _last_version(FrameQueue::VERSION_INIT)
    , _last_msg_timestamp(0)
    , _base_timestamp(0)
    , _reset_base_timestamp(true)
    , _indicator(indicator) {
}

bool DefaultCursor::next(Frame* frame, bool keep_original_time) {
    if (_q == NULL) {
        LOG(ERROR) << "_q is NULL";
        return false;
    }
    const size_t last_version = _last_version;
    const bool has_next = _q->next(frame, &_last_index, &_last_version, _indicator);
    if (last_version != _last_version) {
        // If the version is not matching , it means the frame_queue has 
        // been reset, we need to call reset in DefaultCursor as well to
        // make timestamp in audio/video message sent to client continuous.
        reset();
        return next(frame, keep_original_time);
    }
    if (!has_next) {
        return false;
    }
    if (!keep_original_time) {
        if (_reset_base_timestamp) {
            _reset_base_timestamp = false;
            _base_timestamp = frame->timestamp - _last_msg_timestamp;
        }
        frame->timestamp -= _base_timestamp;
        _last_msg_timestamp = frame->timestamp;
    }
    return true;
}

void DefaultCursor::backup() {
    --_last_index;
}

void DefaultCursor::reset() {
    // NOTE: _last_msg_timestamp must be remained to make time change
    // monotonically.
    _last_index = FrameQueue::INDEX_INIT;
    _reset_base_timestamp = true;
}
