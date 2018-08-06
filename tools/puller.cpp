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
#include "rtmp_forward_service.h"
#include "util.h"
#include <bthread/bthread.h>
#include "butil/logging.h"

DEFINE_string(streams, "", "list of streams, separated with newline/space/colon");
DEFINE_int32(retry_interval_ms, 1000, "Milliseconds between consecutive retries");
DEFINE_bool(share_connection, true, "Multiple RTMP streams over one connection");
DEFINE_int32(dummy_port, 0, "Launch dummy server at this port");
DEFINE_bool(probe, false, "Print several received messages and quit, to check "
            "status of streams");
DEFINE_int32(probe_video_messages, 10, "When -probe is true, quit after "
             "receiving so many video messages");

// Just ignore everything now.
class DummyPlayer : public brpc::RtmpRetryingClientStream {
public:
    DummyPlayer() {}
    void OnMetaData(brpc::RtmpMetaData* metadata, const butil::StringPiece& name) {
        VLOG(99) << "Metadata timestamp=" << metadata->timestamp
                 << " content={" << metadata->data << "}";
    }
    void OnAudioMessage(brpc::RtmpAudioMessage*) {}
    void OnVideoMessage(brpc::RtmpVideoMessage*) {}
};

class ProbingPlayer : public brpc::RtmpRetryingClientStream {
public:
    ProbingPlayer(const std::string& stream_name, int64_t start_play_time)
        : _nv(0), _stop(false), _stream_name(stream_name)
        , _start_play_time(start_play_time) {}

    void OnMetaData(brpc::RtmpMetaData* obj, const butil::StringPiece&) {
        int64_t elp = butil::gettimeofday_ms() - _start_play_time;
        LOG(INFO) << "stream=" << _stream_name << " META real=" << elp
                  << " timestamp=" << obj->timestamp
                  << " content=" << obj->data;
    }
    void OnAudioMessage(brpc::RtmpAudioMessage*) {}
    void OnVideoMessage(brpc::RtmpVideoMessage* msg) {
        if (_nv++ < FLAGS_probe_video_messages) {
            int64_t elp = butil::gettimeofday_ms() - _start_play_time;
            LOG(INFO) << "stream=" << _stream_name << " VIDEO timestamp="
                      << msg->timestamp << " real=" << elp << " size=" << msg->size();
            if (_nv == FLAGS_probe_video_messages) {
                _stop = true;
            }
        }
    }

    bool stopped() const { return _stop; }
    
private:
    int _nv;
    bool _stop;
    std::string _stream_name;
    const int64_t _start_play_time;
};

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    butil::StringPiece host;
    butil::StringPiece port;
    butil::StringPiece vhost;
    butil::StringPiece app;
    butil::StringPiece streams;
    brpc::ParseRtmpURL(FLAGS_streams, &host, &vhost, &port, &app,
                             &streams);
    butil::StringPiece queries;
    streams = brpc::RemoveQueryStrings(streams, &queries);
    for (brpc::QuerySplitter qs(queries); qs; ++qs) {
        if (qs.key() == "vhost") {
            vhost = qs.value();
            break;
        }
    }
    std::string server_addr;
    if (host.empty() || host == "localhost") {
        server_addr = butil::my_hostname();
    } else {
        server_addr = host.as_string();
    }
    if (port.empty()) {
        port = "1935";
    }
    server_addr.push_back(':');
    server_addr.append(port.data(), port.size());
    
    brpc::RtmpClientOptions rtmp_opt;
    std::string app_str = app.as_string();
    rtmp_opt.app = app_str;
    rtmp_opt.tcUrl = "rtmp://";
    if (vhost.empty()) {
        rtmp_opt.tcUrl += server_addr;
    } else {
        rtmp_opt.tcUrl += vhost.as_string();
    }
    rtmp_opt.tcUrl.push_back('/');
    rtmp_opt.tcUrl += app_str;
    brpc::RtmpClient rtmp_client;
    if (rtmp_client.Init(server_addr.c_str(), rtmp_opt) != 0) {
        LOG(ERROR) << "Fail to init rtmp_client";
        return -1;
    }

    if (FLAGS_dummy_port > 0) {
        brpc::StartDummyServerAt(FLAGS_dummy_port);
    }
    std::vector<brpc::RtmpRetryingClientStream*> players;
    butil::StringMultiSplitter sp(streams.begin(), streams.end(), "\n\t ,");
    const int64_t start_play_time = butil::gettimeofday_ms();
    for (; sp; ++sp) {
        std::string stream_name = butil::StringPiece(sp.field(), sp.length()).as_string();
        brpc::RtmpRetryingClientStream* player = NULL;
        if (FLAGS_probe) {
            player = new ProbingPlayer(stream_name, start_play_time);
        } else {
            player = new DummyPlayer;
        }
        brpc::RtmpRetryingClientStreamOptions opt;
        opt.retry_interval_ms = FLAGS_retry_interval_ms;
        opt.share_connection = FLAGS_share_connection;
        opt.play_name = stream_name;
        player->Init(new RtmpSubStreamCreator(&rtmp_client), opt);
        players.push_back(player);
    }
    while (!brpc::IsAskedToQuit()) {
        if (FLAGS_probe) {
            bool all_stopped = true;
            for (size_t i = 0; i < players.size(); ++i) {
                ProbingPlayer* pp = static_cast<ProbingPlayer*>(players[i]);
                if (!pp->stopped()) {
                    all_stopped = false;
                    break;
                }
            }
            if (all_stopped) {
                break;
            }
        }
        usleep(100000);
    }
    LOG(INFO) << "puller is going to quit";
    for (size_t i = 0; i < players.size(); ++i) {
        players[i]->Destroy();
    }
    players.clear();
    return 0;
}
