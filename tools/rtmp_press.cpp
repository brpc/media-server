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

#include <algorithm>
#include <deque>
#include <limits>
#include <pthread.h>
#include <signal.h>
#include <memory>           // std::shared_ptr
#include "brpc/server.h"    // StartDummyServerAt
#include "brpc/socket.h"
#include "brpc/channel.h"
#include "brpc/adaptive_connection_type.h"
#include "bthread/bthread.h"
#include "butil/logging.h"
#include "butil/fast_rand.h"
#include "butil/containers/flat_map.h"
#include "rtmp_forward_service.h"
#include "checksum.h"
#include "scoped_sleep.h"
#include "util.h"

DEFINE_int32(dummy_port, 0, "Launch dummy server at this port");
DEFINE_bool(use_bthread, true, "Use bthread to send requests");
DEFINE_int64(match_wait, 2, "Waiting time(s) before checking");
DEFINE_int64(match_duration, 10, "Time(s) during which the content of publisher and player keeps matching, -1 means forever");
DEFINE_int32(timeout_ms, 3000, "Timeout for reporting");
DEFINE_string(push_server, "", "Address of RTMP push server");
DEFINE_string(play_server, "", "Address of RTMP play server");
DEFINE_string(push_vhost, "", "push vhost");
DEFINE_string(play_vhost, "", "play vhost");
DEFINE_string(app, "live", "SERVER/appName/streamName");
DEFINE_string(stream_name, "test", "SERVER/appName/streamName");
DEFINE_bool(share_connection, false, "multiple RTMP streams over one connection");
DEFINE_int32(stream_num, 1, "Number of publish threads");
DEFINE_int32(player_num, 1, "Number of players to each publisher");
DEFINE_int32(frame_size, 1024, "data size");
DEFINE_int32(fps, 30, "frame per second");
DEFINE_int32(retry_interval_ms, 1000, "Milliseconds between consecutive retries");
DEFINE_int32(gop_size, 30, "Add a keyframe every so many frames");
#ifdef ENABLE_REPORT_LATENCY
DEFINE_string(node_name, "", "The node being played by rtmp_press");
DEFINE_string(report_addr, "", "The report address");
int64_t g_saved_latency = 0;
int64_t g_saved_latency_nnn = 0;
#endif

// The message related with each audio/video data
struct MessageInfo {
    // publish time of this data in publisher
    int64_t publish_us;

    // data index in MessageQueue
    int64_t index;
};

class PublishResult {
public:
    PublishResult()
        : error_players(0)
        , total_players(0)
        , _error_count(0) 
        , _total_count(0) {}
    
    ~PublishResult() {} 
    
    void add_error_count(int64_t error_count) {
        _error_count.fetch_add(error_count, butil::memory_order_relaxed);
    }

    int64_t get_error_count() {
        return _error_count.load(butil::memory_order_relaxed);
    }

    void add_total_count(int64_t total_count) {
        _total_count.fetch_add(total_count, butil::memory_order_relaxed);
    }

    int64_t get_total_count() {
        return _total_count.load(butil::memory_order_relaxed);
    }

    bool is_ok() {
        return (_error_count.load(butil::memory_order_relaxed) == 0 &&
                _total_count.load(butil::memory_order_relaxed) > 0);
    }

    void reset() {
        _error_count.exchange(0);
        _total_count.exchange(0);
    }

public:
    int64_t error_players;
    int64_t total_players;

private:
    butil::atomic<int64_t> _error_count;
    butil::atomic<int64_t> _total_count;
};

class MessageQueue {
public:
    MessageQueue()
        : _min_index(0)
        , _max_index(0) {
        pthread_mutex_init(&_member_mutex, NULL);
    }

    ~MessageQueue() {
        pthread_mutex_destroy(&_member_mutex);
    }

    void push_back(const Checksum& check_sum, int64_t* index) {
        std::unique_lock<pthread_mutex_t> mu(_member_mutex);
        _messages.push_back(check_sum);
        *index = ++_max_index;
    }

    int get_nth(const int64_t n_th, Checksum* check_sum) {
        std::unique_lock<pthread_mutex_t> mu(_member_mutex);
        if (n_th <= _min_index || n_th > _max_index) {
            LOG(ERROR) << "index out of range";
            return -1;
        }
        *check_sum = _messages[n_th - _min_index - 1];
        return 0;
    }

    // min_used_index is the mininum index of received data in players
    // thus we can erase the data before min_used_index
    void erase_unused(const int64_t min_used_index, std::vector<Checksum>& checksums) {
        std::unique_lock<pthread_mutex_t> mu(_member_mutex);
        if (min_used_index < _min_index || min_used_index > _max_index) {
            LOG(ERROR) << "index out of range";
            return;
        }

        checksums.clear();
        checksums.reserve(min_used_index - _min_index);
        for (int64_t i = 0; i < min_used_index - _min_index; ++i) {
            checksums.push_back(_messages.front());
            _messages.pop_front();
            ++_min_index;
        }
    }

    size_t size() {
        return _messages.size();
    }
private:
    pthread_mutex_t _member_mutex;
    std::deque<Checksum> _messages;
    int64_t _min_index;
    int64_t _max_index;
};

struct PublishPlayContext {
public:
    PublishPlayContext()
        : inflight_counter(0)
        , received_anything(false) { 
        if (info_map.init(3000, 70) != 0) {
            LOG(ERROR) << "Fail to init info_map";
        }

        if (pthread_mutex_init(&member_mutex, NULL) != 0) {
            LOG(ERROR) << "Fail to init member_mutex";
        }
    }

    ~PublishPlayContext() {
        if (pthread_mutex_destroy(&member_mutex) != 0) {
            LOG(ERROR) << "Fail to destroy member_mutex";
        }
    }

    butil::atomic<int64_t> inflight_counter;
    MessageQueue video_message_queue;
    MessageQueue audio_message_queue;
    butil::FlatMap<Checksum, MessageInfo> info_map;
    pthread_mutex_t member_mutex;
    bool received_anything;
};

class RtmpPressPublisher : public brpc::RtmpRetryingClientStream {
};

class RtmpPressPlayer : public brpc::RtmpRetryingClientStream {
public:
    RtmpPressPlayer(std::shared_ptr<PublishPlayContext> publish_play_context,
                    PublishResult* publish_result,
                    std::string stream)
        : _last_video_index(0)
        , _last_audio_index(0)
        , _publish_play_context(publish_play_context)
        , _publish_result(publish_result)
        , _total_count(0)
        , _error_count(0)
        , _stream(stream) {}
    
    ~RtmpPressPlayer() {}

    void OnFirstMessage() {
        // LOG(INFO) << "player[" << this << "].stream_id=" << stream_id();
    }

    void OnPlayable() {
        LOG(INFO) << "player[" << this << "].stream_id=" << stream_id() << " is created";
    }
    
    void OnAudioMessage(brpc::RtmpAudioMessage* msg);
    
    void OnVideoMessage(brpc::RtmpVideoMessage* msg);

    int64_t get_last_video_index() {
        // No need to lock since it is ok to read old value
        return _last_video_index;
    }

    int64_t get_last_audio_index() {
        // No need to lock since it is ok to read old value
        return _last_audio_index;
    }

    int64_t get_total_count() { return _total_count; }
    int64_t get_error_count() { return _error_count; }
    std::string get_stream() { return _stream; }

private:
    void OnRawData(const butil::IOBuf& data, 
                   int64_t* last_index,
                   bool is_seq_header,
                   bool is_video);

private:
    int64_t _last_video_index;
    int64_t _last_audio_index;
    std::shared_ptr<PublishPlayContext> _publish_play_context;
    PublishResult* _publish_result;
    int64_t _total_count;
    int64_t _error_count;
    std::string _stream;
};

bool start_recording = false;
bvar::LatencyRecorder g_end2end_latency("end2end");
volatile bool g_signal_quit = false;
void sigint_handler(int) { 
    g_signal_quit = true; 
}

void RtmpPressPlayer::OnAudioMessage(brpc::RtmpAudioMessage* msg) {
    brpc::RtmpAACMessage aac_msg;
    butil::Status st = aac_msg.Create(*msg);
    if (!st.ok()) {
        LOG(ERROR) << st;
        return;
    }
    OnRawData(aac_msg.data, &_last_audio_index,
              aac_msg.packet_type == brpc::FLV_AAC_PACKET_SEQUENCE_HEADER, false);
}

void RtmpPressPlayer::OnVideoMessage(brpc::RtmpVideoMessage* msg) {
    brpc::RtmpAVCMessage avc_msg;
    butil::Status st = avc_msg.Create(*msg);
    if (!st.ok()) {
        LOG(ERROR) << st;
        return;
    }
    OnRawData(avc_msg.data, &_last_video_index,
              avc_msg.packet_type == brpc::FLV_AVC_PACKET_SEQUENCE_HEADER, true);
}

void RtmpPressPlayer::OnRawData(const butil::IOBuf& data, 
                                int64_t* last_index,
                                bool is_seq_header,
                                bool is_video) {
    if (g_signal_quit) return;
    _publish_play_context->received_anything = true;
    _publish_result->add_total_count(1);
    ++_total_count;

    int64_t now_us = butil::gettimeofday_us();
    Checksum checksum;
    checksum.from_iobuf(data);

    std::unique_lock<pthread_mutex_t> mu(_publish_play_context->member_mutex);
    MessageInfo* mi = _publish_play_context->info_map.seek(checksum);
    if (!mi) {
        mu.unlock();
        if (is_seq_header && *last_index != 0) {
            // In this situation, checksum can't be found in map.
            // It is ok because publisher may have deleted this data, while media server still
            // keep it and retransmit it to rtmp_press
            // just do nothing
        } else {
            LOG(ERROR) << "Fail to find checksum:" << checksum
                        << ", last_index=" << *last_index << ", isseq:" << is_seq_header << ", is_video:" << is_video 
                        << ", data=" << data << ", size=" << data.size();
            if (start_recording) {
                ++_error_count;
                _publish_result->add_error_count(1);
            }
        }
        return;
    }
    MessageInfo message_info = *mi;
    mu.unlock();

    const int64_t index = message_info.index;
    int sub_count = 1;
    // check index
    if (index != *last_index + 1) {
        if (index <= *last_index) {
            // OK to resend sequence header.
            if (!is_seq_header) {
                LOG(ERROR) << "frame #" << index << " is already received or skipped, expecting frame #" << *last_index + 1;
                goto FAIL;
            }
            return;
        }
        if (*last_index + 2 < index) {
            LOG(ERROR) << "frame #" << *last_index + 1 << " to #" << index - 1
                       << " is skipped";
        } else {
            LOG(ERROR) << "frame #" << *last_index + 1 << " is skipped";
        }
        if (start_recording) {
            ++_error_count;
            _publish_result->add_error_count(1);
        }
        sub_count = index - *last_index;
    }
    *last_index = index;

    // check the computed checksum
    Checksum checksum_pub;
    if (is_video && _publish_play_context->video_message_queue.get_nth(index, &checksum_pub) != 0) {
        LOG(ERROR) << "Fail to Get message from _message_queue";
        goto FAIL;
    }
    if (!is_video && _publish_play_context->audio_message_queue.get_nth(index, &checksum_pub) != 0) {
        LOG(ERROR) << "Fail to Get message from _message_queue";
        goto FAIL;
    }
    if (checksum_pub != checksum) {
        LOG(ERROR) << "Fail to match checksum of index:" << index;
        goto FAIL;
    }
    _publish_play_context->inflight_counter.fetch_sub(sub_count, butil::memory_order_relaxed);
    if (is_video) {
        g_end2end_latency << now_us - message_info.publish_us;
    }
    return;

    FAIL:
    if (start_recording) {
        ++_error_count;
        _publish_result->add_error_count(1);
    }
}

void* publish_thread(void* arg) {
    PublishResult* publish_result = (PublishResult *)arg;
    std::shared_ptr<PublishPlayContext> publish_play_context(new PublishPlayContext);

    // Initialize rtmp_push_client
    std::string push_server_addr = FLAGS_push_server;
    if (push_server_addr.empty()) {
        push_server_addr = butil::string_printf("%s:8079", butil::my_hostname());
    } else if (push_server_addr.find(':') == std::string::npos) {
        push_server_addr += ":1935";
    }
    brpc::RtmpClientOptions rtmp_opt;
    rtmp_opt.app = FLAGS_app;
    rtmp_opt.tcUrl = "rtmp://";
    if (FLAGS_push_vhost.empty()) {
        rtmp_opt.tcUrl += push_server_addr;
    } else {
        rtmp_opt.tcUrl += FLAGS_push_vhost;
    }
    rtmp_opt.tcUrl.push_back('/');
    rtmp_opt.tcUrl += FLAGS_app;
    brpc::RtmpClient rtmp_push_client;
    if (rtmp_push_client.Init(push_server_addr.c_str(), rtmp_opt) != 0) {
        LOG(ERROR) << "Fail to init rtmp_push_client";
        return NULL;
    }

    // Initialize rtmp_play_client
    std::vector<brpc::RtmpClient> play_clients;
    std::vector<std::string> remote_sides;
    butil::StringSplitter sp(FLAGS_play_server.c_str(), ' ');
    for (; sp; ++sp) {
        std::string play_server_addr(sp.field(), sp.length());
        if (play_server_addr.empty()) {
            play_server_addr = butil::string_printf("%s:8079", butil::my_hostname());
        } else if (play_server_addr.find(':') == std::string::npos) {
            play_server_addr += ":1935";
        }
        brpc::RtmpClientOptions play_rtmp_opt;
        play_rtmp_opt.app = FLAGS_app;
        play_rtmp_opt.tcUrl = "rtmp://";
        if (FLAGS_play_vhost.empty()) {
            play_rtmp_opt.tcUrl += play_server_addr;
        } else {
            play_rtmp_opt.tcUrl += FLAGS_play_vhost;
        }
        play_rtmp_opt.tcUrl.push_back('/');
        play_rtmp_opt.tcUrl += FLAGS_app;
        brpc::RtmpClient rtmp_play_client;
        if (rtmp_play_client.Init(play_server_addr.c_str(), play_rtmp_opt) != 0) {
            LOG(ERROR) << "Fail to init rtmp_push_client";
            return NULL;
        }
        play_clients.push_back(rtmp_play_client);
        remote_sides.push_back(play_server_addr);
    }

    const std::string stream_name = butil::string_printf(
        "%s_%lu", FLAGS_stream_name.c_str(), (long)butil::fast_rand());

    butil::intrusive_ptr<RtmpPressPublisher> publisher(new RtmpPressPublisher);
    brpc::RtmpRetryingClientStreamOptions opt;
    opt.retry_interval_ms = FLAGS_retry_interval_ms;
    opt.share_connection = FLAGS_share_connection;
    opt.publish_name = stream_name;
    opt.publish_type = brpc::RTMP_PUBLISH_LIVE;
    opt.wait_until_play_or_publish_is_sent = true;
    opt.quit_when_no_data_ever = false;
    publisher->Init(new RtmpSubStreamCreator(&rtmp_push_client), opt);
    LOG(INFO) << "publisher.stream_id=" << publisher->stream_id();

    int total_players = FLAGS_player_num * play_clients.size();
    butil::intrusive_ptr<RtmpPressPlayer>* players =
            new butil::intrusive_ptr<RtmpPressPlayer>[total_players];
    for (size_t i = 0; i < play_clients.size(); ++i) {
        for (int j = 0; j < FLAGS_player_num; ++j) {
            int k = i * FLAGS_player_num + j;
            players[k].reset(new RtmpPressPlayer(publish_play_context, publish_result, remote_sides[i]));
            brpc::RtmpRetryingClientStreamOptions opt;
            opt.retry_interval_ms = FLAGS_retry_interval_ms;
            opt.share_connection = FLAGS_share_connection;
            opt.play_name = stream_name;
            opt.quit_when_no_data_ever = false;
            players[k]->Init(new RtmpSubStreamCreator(&play_clients[i]), opt);
        }
    }

    const int64_t base_ms = butil::gettimeofday_ms();
    brpc::RtmpAVCMessage avc_msg;
    brpc::RtmpAACMessage aac_msg;
    std::string raw_audio_data;
    std::string raw_video_data;
    raw_audio_data.resize(FLAGS_frame_size);
    raw_video_data.resize(FLAGS_frame_size);

    // congestion control policy:
    // If the number of inflight packets is larger than fps * 5,
    // then the program just waits until the value becomes normal
    int max_inflight = FLAGS_fps * 5;
    int erase_unused_cycle = 0;
    int64_t video_cyc_count = 0;
    bool is_first = true;

    while (!g_signal_quit) {
        ScopedSleep scoped_sleep(1000000/FLAGS_fps);
        const int64_t now_us = butil::gettimeofday_us();
        if (publish_play_context->received_anything) {
            if ((publish_play_context->inflight_counter.load(butil::memory_order_relaxed)
                 * FLAGS_stream_num) > max_inflight) {
                bthread_usleep(1000);
                continue;
            }
        } else {
            bthread_usleep(1000);
        }
        generate_fake_header(avc_msg, aac_msg, now_us / 1000 - base_ms,
                             is_first, video_cyc_count++, FLAGS_gop_size);
        if (is_first) {
            is_first = false;
        }
        generate_random_string(raw_video_data);
        generate_random_string(raw_audio_data);

        Checksum check_sum;
        int64_t index;

        {
            std::unique_lock<pthread_mutex_t> mu(publish_play_context->member_mutex);
            check_sum.from_string(raw_video_data);
            publish_play_context->video_message_queue.push_back(check_sum, &index);
            publish_play_context->info_map[check_sum] = MessageInfo{now_us, index};

            check_sum.from_string(raw_audio_data);
            publish_play_context->audio_message_queue.push_back(check_sum, &index);
            publish_play_context->info_map[check_sum] = MessageInfo{now_us, index};
         }

        avc_msg.data.clear();
        avc_msg.data.append(raw_video_data);
        while (publisher->SendAVCMessage(avc_msg) != 0 && !g_signal_quit) {
            bthread_usleep(10000);
        }

        aac_msg.data.clear();
        aac_msg.data.append(raw_audio_data);
        while (publisher->SendAACMessage(aac_msg) != 0 && !g_signal_quit) {
            bthread_usleep(10000);
        }
        publish_play_context->inflight_counter.fetch_add(FLAGS_player_num, butil::memory_order_relaxed);

        //erase unused data every second
        ++erase_unused_cycle;
        if (erase_unused_cycle == FLAGS_fps) {
            erase_unused_cycle = 0;
            int64_t min_video_index = std::numeric_limits<int64_t>::max();
            int64_t min_audio_index = std::numeric_limits<int64_t>::max();
            for (int i = 0; i < total_players; ++i) {
                int64_t last_video_index = players[i]->get_last_video_index();
                int64_t last_audio_index = players[i]->get_last_audio_index();

                min_video_index = (last_video_index < min_video_index)? last_video_index: min_video_index;
                min_audio_index = (last_audio_index < min_audio_index)? last_audio_index: min_audio_index;
            }
            std::vector<Checksum> video_checksums;
            std::vector<Checksum> audio_checksums;
            publish_play_context->video_message_queue.erase_unused(min_video_index, video_checksums);
            publish_play_context->audio_message_queue.erase_unused(min_audio_index, audio_checksums);
            {
                std::unique_lock<pthread_mutex_t> mu(publish_play_context->member_mutex);
                for (auto it = video_checksums.begin(); it != video_checksums.end(); ++it) {
                    publish_play_context->info_map.erase(*it);
                }
                for (auto it = audio_checksums.begin(); it != audio_checksums.end(); ++it) {
                    publish_play_context->info_map.erase(*it);
                }
            }
            VLOG(99) << "video size:" << publish_play_context->video_message_queue.size();
        }
    }

    publish_result->total_players = total_players;
    double error_rate = 0;
    for (int i = 0; i < total_players; ++i) {
        if (players[i]->get_total_count() == 0) {
            ++publish_result->error_players;
            LOG(ERROR) << "Remote side=" << players[i]->get_stream()
                       << " cannot be accessed";
        } else if ((error_rate = static_cast<double>(players[i]->get_error_count()) /
                players[i]->get_total_count()) > 0.1) {
            ++publish_result->error_players;
            LOG(ERROR) << "Remote side=" << players[i]->get_stream()
                      << " error rate=" << error_rate << " too high";
        }
    }

    return NULL;
}

void* report_thread(void* arg) {
    std::vector<PublishResult>* publish_results = (std::vector<PublishResult> *)arg;
    int report_latency_cycle = 0;
    int report_latency_interval = 5;
    std::string control_tab = "\t\t\t\t";
    while (!g_signal_quit) {
        bthread_usleep(1000000 /* 1s */);
        int64_t error_count = 0;
        for (size_t i = 0; i < publish_results->size(); ++i) {
            error_count += publish_results->at(i).get_error_count();
        }
        LOG(INFO) << "fps=" << g_end2end_latency.qps(1) << ", total_error=" << error_count;

        ++report_latency_cycle;
        if (report_latency_cycle == report_latency_interval) {
            report_latency_cycle = 0;
            #ifdef ENABLE_REPORT_LATENCY
            g_saved_latency = g_end2end_latency.latency_percentile(0.50);
            g_saved_latency_nnn = g_end2end_latency.latency_percentile(0.999);
            #endif
            LOG(INFO) << "[latency in us]\n"
                    << "avg" << control_tab << g_end2end_latency.latency(1) << "\n"
                    << "50%" << control_tab << g_end2end_latency.latency_percentile(0.50) << "\n"
                    << "70%" << control_tab << g_end2end_latency.latency_percentile(0.70) << "\n"
                    << "90%" << control_tab << g_end2end_latency.latency_percentile(0.90) << "\n"
                    << "95%" << control_tab << g_end2end_latency.latency_percentile(0.95) << "\n"
                    << "97%" << control_tab << g_end2end_latency.latency_percentile(0.97) << "\n"
                    << "99%" << control_tab << g_end2end_latency.latency_percentile(0.99) << "\n"
                    << "99.9%" << control_tab << g_end2end_latency.latency_percentile(0.999) << "\n"
                    << "99.99%" << control_tab << g_end2end_latency.latency_percentile(0.9999) << "\n"
                    << "max" << control_tab << g_end2end_latency.latency_percentile(1.0);
        }
    }

    return NULL;
}
int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_player_num <= 0) {
        LOG(ERROR) << "Invalid player_num=" << FLAGS_player_num;
        return -1;
    }

    if (FLAGS_dummy_port > 0) {
        brpc::StartDummyServerAt(FLAGS_dummy_port);
    }

    bthread_t tid_report;
    pthread_t pid_report;
    std::vector<bthread_t> tids;
    std::vector<pthread_t> pids;
    tids.resize(FLAGS_stream_num);
    pids.resize(FLAGS_stream_num);
    std::vector<PublishResult> publish_results(FLAGS_stream_num);

    if (!FLAGS_use_bthread) {
        for (int i = 0; i < FLAGS_stream_num; ++i) {
            if (pthread_create(&pids[i], NULL, publish_thread, &publish_results[i]) != 0) {
                LOG(ERROR) << "Fail to create pthread";
                return -1;
            }
        }
        if (pthread_create(&pid_report, NULL, report_thread, &publish_results) != 0) {
            LOG(ERROR) << "Fail to create control thread";
            return -1;
        }
    } else {
        for (int i = 0; i < FLAGS_stream_num; ++i) {
            if (bthread_start_background(&tids[i], NULL, publish_thread, &publish_results[i]) != 0) {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
        if (bthread_start_background(&tid_report, NULL, report_thread, &publish_results) != 0) {
            LOG(ERROR) << "Fail to create control thread";
            return -1;
        }
    }

    signal(SIGINT, sigint_handler);
    const int64_t base_ms = butil::gettimeofday_ms();
    while (!g_signal_quit) {
        int64_t now = butil::gettimeofday_ms();
        if (now < base_ms + FLAGS_match_wait * 1000) {
            bthread_usleep(10000);
            continue;
        }
        start_recording = true;
        LOG(INFO) << "start_recording";
        break;
    }

    // -1 means runs forever
    while (!g_signal_quit && FLAGS_match_duration != -1) {
        int64_t now = butil::gettimeofday_ms();
        if (now < base_ms + FLAGS_match_wait * 1000 + FLAGS_match_duration * 1000) {
            bthread_usleep(10000);
            continue;
        }
        g_signal_quit = true;
        break;
    }

    for (int i = 0; i < FLAGS_stream_num; ++i) {
        if (!FLAGS_use_bthread) {
            pthread_join(pids[i], NULL);
        } else {
            bthread_join(tids[i], NULL);
        }
    }

    if (!FLAGS_use_bthread) {
        pthread_join(pid_report, NULL);
    } else {
        bthread_join(tid_report, NULL);
    }

    int succ_num = 0;
    int fail_num = 0;
    for (int i = 0; i < FLAGS_stream_num; ++i) {
        if (publish_results[i].is_ok()) {
            succ_num++;
        } else {
            fail_num++;
        }
    }

#ifdef ENABLE_REPORT_LATENCY
    char hostname[32];
    if (!gethostname(hostname, sizeof(hostname))) {
        //generate_random_string(hostname, 16);
        strcpy(hostname, "Machine1");
    }
    brpc::Channel report_channel;
    brpc::ChannelOptions opt;
    opt.protocol = brpc::PROTOCOL_HTTP;
    opt.connect_timeout_ms = FLAGS_timeout_ms / 3;
    opt.timeout_ms = FLAGS_timeout_ms;
    if (report_channel.Init(FLAGS_report_addr, "", &opt) != 0) {
        LOG(ERROR) << "Fail to init channel to tdsb";
        return -1;
    }

    std::ostringstream post_data;
    post_data << "machine=" << hostname 
              << "&node=" << FLAGS_node_name 
              << "&latency=" << g_saved_latency 
              << "&ratio=" << ((g_saved_latency == 0)? 0: (static_cast<double>(g_saved_latency_nnn) / g_saved_latency));
    LOG(INFO) << "post_data=" << post_data.str();

    brpc::Controller cntl;
    cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
    cntl.http_request().uri() = "/posturl";
    cntl.request_attachment().append(post_data.str());
    report_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Unable to report latency"
                   << ", res=" << cntl.ErrorCode();
    }
#endif
    exit(fail_num != 0);
}
