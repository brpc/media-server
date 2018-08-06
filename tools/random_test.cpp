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

#include "brpc/server.h"
#include "brpc/socket.h"
#include "brpc/adaptive_connection_type.h"
#include "butil/logging.h"
#include <vector>
#include <bthread/bthread.h>
#include <random>
#include "rtmp_forward_service.h"
#include "frame_queue.h"
#include "scoped_sleep.h"
#include "util.h"

DEFINE_string(play_server, "", "The server to pull streams");
DEFINE_string(push_server, "", "The server to push streams");
DEFINE_string(vhost, "", "vhost of pushed or pulled streams");
DEFINE_string(app, "live", "App name of pushed or pulled streams");
DEFINE_int32(retry_interval_ms, 1000, "Milliseconds between consecutive retries");
DEFINE_int32(dummy_port, 0, "Launch dummy server at this port");
DEFINE_uint64(max_stream_num, 80, "");
DEFINE_uint64(min_stream_num, 30, "");
DEFINE_int32(fps, 30, "frame per second");
DEFINE_int32(frame_size, 4096, "data size");
DEFINE_int32(gop_size, 80, "Add a keyframe every so many frames");
DEFINE_int64(test_duration_s, 20, "Random test will last for such time(in second), -1 means running forever");

class Publisher : public brpc::RtmpRetryingClientStream {
};

// Just ignore everything now.
class DummyPlayer : public brpc::RtmpRetryingClientStream {
public:
    DummyPlayer() {}

    void OnMetaData(brpc::RtmpMetaData*, const butil::StringPiece&) {}
    void OnAudioMessage(brpc::RtmpAudioMessage*) {}
    void OnVideoMessage(brpc::RtmpVideoMessage*) {}
};

// Stream represents a fake publisher and several fake players
struct Stream {
    butil::intrusive_ptr<Publisher> publisher;
    std::vector<butil::intrusive_ptr<DummyPlayer>> dummy_player;
    std::string stream_name;
};

class RandomGenerator {
public:
    RandomGenerator()
        : _video_cyc_count(0)
        , _is_first(true) {
        pthread_mutex_init(&_member_mutex, NULL);     
    }

    ~RandomGenerator() {
        pthread_mutex_destroy(&_member_mutex);
        for (size_t i = 0; i < _streams.size(); ++i) {
            for (size_t j = 0; j < _streams[i].dummy_player.size(); j++) {
                _streams[i].dummy_player[j]->Destroy();
            }
            _streams[i].publisher->Destroy();
        }
        _streams.clear();
    }

    int Init() {
        bthread_t bt;
        if (bthread_start_background(&bt, NULL, run_publish, this) != 0 ) {
            LOG(ERROR) << "Fail to create _publish_thread";
            return -1;
        }
        return 0;
    }

    int create_player(const std::string &stream, 
                      const brpc::RtmpClient& client,
                      butil::intrusive_ptr<DummyPlayer> *pplayer) {
        butil::intrusive_ptr<DummyPlayer> player(new DummyPlayer);
        brpc::RtmpRetryingClientStreamOptions opt;
        opt.retry_interval_ms = FLAGS_retry_interval_ms;
        opt.share_connection = butil::fast_rand_less_than(2) == 0;
        opt.play_name = stream;
        player->Init(new RtmpSubStreamCreator(&client), opt);
        if (pplayer) {
            *pplayer = player;
        }
        return 0;
    }

    int create_publisher(const std::string &stream, 
                         const brpc::RtmpClient& publish_client,
                         butil::intrusive_ptr<Publisher> *publisher_out) {
        butil::intrusive_ptr<Publisher> publisher(new Publisher);
        brpc::RtmpRetryingClientStreamOptions pub_opt;
        pub_opt.retry_interval_ms = FLAGS_retry_interval_ms;
        pub_opt.share_connection = butil::fast_rand_less_than(2) == 0;
        pub_opt.publish_name = stream;
        pub_opt.publish_type = brpc::RTMP_PUBLISH_LIVE;
        pub_opt.quit_when_no_data_ever = false;
        publisher->Init(new RtmpSubStreamCreator(&publish_client), pub_opt);
        if (publisher_out) {
            *publisher_out = publisher;
        }
        return 0;
    }

    void add_stream(const std::string &stream_name,
                    const brpc::RtmpClient& publish_client,
                    const brpc::RtmpClient& client) {
        {
            std::unique_lock<pthread_mutex_t> mu(_member_mutex);
            if (_streams.size() > FLAGS_max_stream_num) {
                return;
            }
        }
        Stream stream;
        create_publisher(stream_name, publish_client, &stream.publisher);
        size_t size = butil::fast_rand_less_than(3); // [0,2]
        butil::intrusive_ptr<DummyPlayer> dp;
        bthread_usleep(100 * 1000L);
        for (size_t j = 0; j < size; j++) {
            create_player(stream_name, client, &dp);
            stream.dummy_player.push_back(dp);
        }
        // To simulate abnormal players
        for (size_t j = 0; j < size; j++) {
            create_player(stream_name + "_abnormal", client, &dp);
            stream.dummy_player.push_back(dp);
        }
        stream.stream_name = stream_name;
        {
            std::unique_lock<pthread_mutex_t> mu(_member_mutex);
            _streams.push_back(stream);
        }
    }

    void delete_stream() {
        std::unique_lock<pthread_mutex_t> mu(_member_mutex);
        if (_streams.size() <= FLAGS_min_stream_num) {
            return;
        }
        size_t i = butil::fast_rand_less_than(_streams.size());
        std::swap(_streams[i], _streams.back());
        Stream& stream = _streams.back();
        for (size_t j = 0; j < stream.dummy_player.size(); j++) {
            stream.dummy_player[j]->Destroy();
        }
        stream.dummy_player.clear();
        stream.publisher->Destroy();
        _streams.pop_back();
    }

    void add_player(const brpc::RtmpClient& client) {
        std::unique_lock<pthread_mutex_t> mu(_member_mutex);
        size_t i = butil::fast_rand_less_than(_streams.size());
        butil::intrusive_ptr<DummyPlayer> dp;
        create_player(_streams[i].stream_name, client, &dp);
        _streams[i].dummy_player.push_back(dp);
    }

    void delete_player() {
        std::unique_lock<pthread_mutex_t> mu(_member_mutex);
        size_t i = butil::fast_rand_less_than(_streams.size());
        if (_streams[i].dummy_player.size() > 0) {
            _streams[i].dummy_player.back()->Destroy();
            _streams[i].dummy_player.pop_back();
        }
    }

private:
    std::vector<Stream> _streams;
    pthread_mutex_t _member_mutex;
    int64_t _video_cyc_count;
    bool _is_first;

private:
    static void* run_publish(void* arg);
    void publish();
    void generate_header(brpc::RtmpAVCMessage& avc_msg,
                         brpc::RtmpAACMessage& aac_msg,
                         int64_t timestamp_ms,
                         int64_t gop_size) {
        generate_fake_header(avc_msg, aac_msg, timestamp_ms,
                             _is_first, _video_cyc_count++, gop_size);
        if (_is_first) {
            _is_first = false; 
        }
    }
};

void* RandomGenerator::run_publish(void* arg) {
    static_cast<RandomGenerator*>(arg)->publish();
    return NULL;
}

void RandomGenerator::publish() {
    const int64_t base_ms = butil::gettimeofday_ms();
    std::string raw_audio_data;
    std::string raw_video_data;
    int32_t cur_frame_size = FLAGS_frame_size;
    int32_t fps = FLAGS_fps;
    int64_t gop_size = FLAGS_gop_size;

    raw_video_data.resize(cur_frame_size);
    raw_audio_data.resize(cur_frame_size / 9 + 1);
    while (!brpc::IsAskedToQuit()) {
        ScopedSleep scoped_sleep(1000000/fps);
        brpc::RtmpAVCMessage avc_msg;
        brpc::RtmpAACMessage aac_msg;
        const int64_t now_ms = butil::gettimeofday_ms();
        generate_header(avc_msg, aac_msg, now_ms - base_ms, gop_size);
        generate_random_string(raw_video_data);
        generate_random_string(raw_audio_data);

        avc_msg.data.clear();
        aac_msg.data.clear();
        avc_msg.data.append(raw_video_data);
        aac_msg.data.append(raw_audio_data);
        std::unique_lock<pthread_mutex_t> mu(_member_mutex);
        for (size_t i = 0; i < _streams.size(); ++i) {
            _streams[i].publisher->SendAVCMessage(avc_msg);
            _streams[i].publisher->SendAACMessage(aac_msg);
        }
    }
}

enum RandomOperation {
    ADD_STREAM,
    DEL_STREAM,
    ADD_PLAYER,
    DEL_PLAYER
};

RandomOperation select_work() {
    size_t s = butil::fast_rand_less_than(100);
    if (s < 12) {
        return ADD_STREAM;
    } else if (s < 30) {
        return DEL_STREAM;
    } else if (s < 60) {
        return ADD_PLAYER;
    } else {
        return DEL_PLAYER;
    }
}


int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_dummy_port > 0) {
        brpc::StartDummyServerAt(FLAGS_dummy_port);
    }

    /* init rtmp client of publisher */
    std::string push_server = FLAGS_push_server;
    if (push_server.empty()) {
        push_server = butil::string_printf("%s:8079", butil::my_hostname());
    } else if (push_server.find(':') == std::string::npos) {
        push_server += ":1935";
    }
    brpc::RtmpClientOptions rtmp_opt;
    rtmp_opt.app = FLAGS_app;
    rtmp_opt.tcUrl = "rtmp://";
    if (FLAGS_vhost.empty()) {
        rtmp_opt.tcUrl += push_server;
    } else {
        rtmp_opt.tcUrl += FLAGS_vhost;
    }
    rtmp_opt.tcUrl.push_back('/');
    rtmp_opt.tcUrl += FLAGS_app;
    brpc::RtmpClient publish_client;
    if (publish_client.Init(push_server.c_str(), rtmp_opt) != 0) {
        LOG(ERROR) << "Fail to init publish_client";
        return -1;
    }

    /* init rtmp client of dummy player */
    std::string play_server = FLAGS_play_server;
    if (play_server.empty()) {
        play_server = butil::string_printf("%s:8079", butil::my_hostname());
    } else if (play_server.find(':') == std::string::npos) {
        play_server += ":1935";
    }
    brpc::RtmpClientOptions play_rtmp_opt;
    play_rtmp_opt.app = FLAGS_app;
    play_rtmp_opt.tcUrl = "rtmp://";
    if (FLAGS_vhost.empty()) {
        play_rtmp_opt.tcUrl += play_server;
    } else {
        play_rtmp_opt.tcUrl += FLAGS_vhost;
    }
    play_rtmp_opt.tcUrl.push_back('/');
    play_rtmp_opt.tcUrl += FLAGS_app;
    brpc::RtmpClient rtmp_client;
    if (rtmp_client.Init(play_server.c_str(), play_rtmp_opt) != 0) {
        LOG(ERROR) << "Fail to init rtmp_client";
        return -1;
    }

    size_t stream_num = 40;
    RandomGenerator rg;
    rg.Init();
    for (size_t i = 0; i < stream_num; i++) { 
        rg.add_stream(butil::string_printf("%lu", (long)butil::fast_rand()),
                      publish_client, rtmp_client);
    }

    int64_t base_time = butil::gettimeofday_s();
    while (!brpc::IsAskedToQuit()) {
        int work = select_work();
        switch (work) {
            case ADD_STREAM:
                rg.add_stream(butil::string_printf("%lu", (long)butil::fast_rand()),
                              publish_client, 
                              rtmp_client);
                break;
            case DEL_STREAM:
                rg.delete_stream();
                break;
            case ADD_PLAYER:
                rg.add_player(rtmp_client);
                break;
            case DEL_PLAYER:
                rg.delete_player();
                break;
        }
        int64_t now = butil::gettimeofday_s();
        if (FLAGS_test_duration_s != -1) {
            if (base_time + FLAGS_test_duration_s < now) {
                break;
            }
        }
        bthread_usleep(100000);
    }
    LOG(INFO) << "random_test is going to quit";
    return 0;
}
