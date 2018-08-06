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

#ifndef MEDIA_SERVER_FRAME_QUEUE_H
#define MEDIA_SERVER_FRAME_QUEUE_H

#include <deque>
#include <boost/shared_ptr.hpp>
#include "brpc/rtmp.h"

enum FrameType {
    FRAME_UNKNOWN = 0,
    FRAME_VIDEO,
    FRAME_AUDIO,
    FRAME_USER_DATA,
};

// Element in FrameQueue.
struct Frame {
    FrameType type;
    uint32_t timestamp;

    // The corrected time this frame has. In the time back-off case,
    // this value is set to the corrected_time of last frame plus a
    // default interval to make time increase monotonically.
    // It is used for implementing min_cache_length and max_cache_length.
    int64_t corrected_time;

    // Generated and set by FrameQueue.
    size_t index;
    // video headers
    brpc::FlvVideoFrameType frame_type;
    brpc::FlvVideoCodec vcodec;
    // sound headers
    brpc::FlvAudioCodec acodec;
    brpc::FlvSoundRate rate;
    brpc::FlvSoundBits bits;
    brpc::FlvSoundType sound_type;
    // raw video/audio data
    butil::IOBuf data;

    // metadata/headers assoicated with this frame. This is set for all
    // frames so that new players coming are sent with these stuff first.
    boost::shared_ptr<brpc::RtmpMetaData> metadata;
    boost::shared_ptr<brpc::RtmpVideoMessage> avc_seq_header;
    boost::shared_ptr<brpc::RtmpAudioMessage> aac_seq_header;
    
    Frame();
    Frame(brpc::RtmpVideoMessage& msg);
    Frame(brpc::RtmpAudioMessage& msg);

    bool swap_as(brpc::RtmpVideoMessage& msg);
    bool swap_as(brpc::RtmpAudioMessage& msg);

    bool is_keyframe() const;
    
    // In-wire size.
    size_t size() const { return data.size() + 1; }
};

class FrameQueueUserConfig {
public:
    virtual ~FrameQueueUserConfig() {}

    // In some special streams, we customize the cache_length of frame
    // queue by implementing this function.
    virtual int64_t get_cache_length() = 0;

    // When poping out frames from frame queue, the return value of this
    // function should also be checked together with min/max_cache_length.
    // In default case, you could implement this function as just return 
    // true indicating we don't care about the minimun number of frames.
    // `current_count' is the size of frame_queue.
    virtual bool has_enough_frame(int64_t current_count) = 0;

    // In some cases, frames are not found by min_cache_length or max_cache_length.
    // Instead we want to get first frame in a customized way, such as the nth frame
    // from the newest frames.
    // `frame_count' is a parameter from user.
    virtual int64_t get_first_frame_count(int64_t frame_count) = 0;

    // In some cases, frames are not found by min_cache_length or max_cache_length.
    // Instead we want to get first frame in a customized way, such as the frame
    // in the frame queue for at least n seconds specified by this function.
    // `play_back_time_s' is a parameter from user.
    virtual int64_t get_play_back_time_s(int64_t play_back_time_s) = 0;
};

// Indicate the way how the frame is taken out from frame_queue
class FrameIndicator {
public:
    FrameIndicator();
    ~FrameIndicator() {}

    enum Type {
        BY_CONFIG = 0,
        BY_PLAY_BACK,
        BY_FIRST_FRAME_COUNT,
        BY_USING_LATEST,
    };

public:
    Type type;
    int64_t play_back_time_s;
    int64_t first_frame_count;
};

// The queue storing video/audio frames that are about to be dispatched
// to other streams.
class FrameQueue {
public:
    struct Stats {
        // Index of the latest message pushed.
        size_t max_index;

        // # of frames cached.
        size_t nframes;

        // # of keyframes cached.
        int nkeyframes;

        // timestamp of last message pushed. If no message was ever pushed,
        // this field is 0.
        uint32_t last_msg_timestamp;

        // Last time when push() was called. If push() was never called,
        // this field is 0.
        int64_t last_push_realtime_us;
        
        // last time when ctor/reset was called.
        int64_t last_reset_realtime_us;
    };

    static const size_t INDEX_INIT = 0;
    static const size_t VERSION_INIT = 0;
    
    FrameQueue();
    ~FrameQueue();
    
    // Push a frame into the queue.
    // Returns true on successfully pushed, false otherwise.
    bool push(Frame& frame, const void* pusher);
    bool push(Frame& frame) { return push(frame, NULL); }

    // Put next frame in *frame.
    // `index' is a unique identifier of a frame. If the frame denoted by
    // *index+1 exists in the queue, the frame is returned; otherwise frames
    // are scanned from newest to oldest by checking `skipper' (if not NULL)
    // to jump over unneeded frames.
    // Put _version in *version.
    // indicator tells the way how the frame is taken out from frame_queue
    // Returns true on success, false otherwise.
    bool next(Frame* frame, size_t* index, size_t* version, 
              const FrameIndicator& indicator);

    // Call this function when the publisher is changed.
    // The `allowed_pusher' is for checking identity (atomically).
    void reset(const void* allowed_pusher);

    // Get statistics, for monitoring purposes.
    void get_stats(Stats*);

    bool has_keyframe() const { return !_keyframes.empty(); }

    void set_name(const std::string& name) { _name = name; }

    void set_user_config(FrameQueueUserConfig* config)
    { _user_config.reset(config); }

private:
    std::string _name; // for debugging
    pthread_mutex_t _mutex;
    size_t _max_index;
    size_t _version; // +1 iff in reset()
    int64_t _last_push_realtime_us;
    int64_t _last_reset_realtime_us;
    uint32_t _last_msg_timestamp;
    uint32_t _last_msg_corrected_time;
    const void* _allowed_pusher;
    
    // One (minor) issue of deque is that it allocates and deallocates memory
    // constantly during pushing and popping. However we can't use fixed-sized
    // BoundedQueue here because the queue size must be dynamically adjusted
    // according to GOP size.
    // CAUTION: when using another container, make sure memory of values in
    // the container are not invalidated after resizing, because _keyframes
    // stores pointers to the values.
    std::deque<Frame> _frames;
    
    // Storing pointers to keyframes in _frames, for finding the keyframe 
    // in implementing min_cache_length and max_cache_length
    std::deque<const Frame*> _keyframes;

    boost::shared_ptr<FrameQueueUserConfig> _user_config;
};

class FrameCursor {
public:
    virtual ~FrameCursor() {}
    
    // Put next frame in *frame.
    // Returns true on success, false otherwise.
    virtual bool next(Frame* frame, bool keep_original_time) = 0;

    // Step backward so that the frame returned by previous Next() will be
    // returned again.
    virtual void backup() = 0;

    // Reset to the states just constructed.
    virtual void reset() = 0;
};

// The default impl. of FrameCursor in media_server.
class DefaultCursor : public FrameCursor {
public:
    explicit DefaultCursor(FrameQueue* q, const FrameIndicator& indicator);
    ~DefaultCursor() {}
    bool next(Frame* frame, bool keep_original_time);
    void backup();
    void reset();
    size_t last_index() const { return _last_index; }
private:
    FrameQueue* _q;
    size_t _last_index;
    size_t _last_version;
    uint32_t _last_msg_timestamp;
    uint32_t _base_timestamp;
    bool _reset_base_timestamp;
    FrameIndicator _indicator;
};

#endif // MEDIA_SERVER_FRAME_QUEUE_H
