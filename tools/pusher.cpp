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
#include "frame_queue.h"
#include "util.h"
#include <bthread/bthread.h>
#include "butil/logging.h"

DEFINE_string(streams, "", "list of streams, separated with newline/space/colon");
DEFINE_bool(pull_share_conn, true, "Multiple RTMP streams over one connection");
DEFINE_string(push_to, "", "The server to push streams");
DEFINE_string(push_app, "", "App name of pushed streams");
DEFINE_string(push_vhost, "", "vhost of pushed streams");
DEFINE_string(push_stream, "", "stream name of pushed streams");
DEFINE_bool(push_share_conn, true, "Multiple RTMP streams over one connection");
DEFINE_int32(retry_interval_ms, 1000, "Milliseconds between consecutive retries");
DEFINE_int32(dummy_port, 0, "Launch dummy server at this port");

class Publisher : public brpc::RtmpRetryingClientStream {
};

class Player : public brpc::RtmpRetryingClientStream {
public:
    explicit Player(Publisher* publisher);
    ~Player();
    
    void OnMetaData(brpc::RtmpMetaData*, const butil::StringPiece&);
    void OnAudioMessage(brpc::RtmpAudioMessage*);
    void OnVideoMessage(brpc::RtmpVideoMessage*);

private:
    void flush();
    
    Publisher* _publisher;
    FrameQueue _frame_queue;
    FrameCursor* _cursor;
    std::shared_ptr<brpc::RtmpMetaData> _metadata;
    std::shared_ptr<brpc::RtmpVideoMessage> _avc_seq_header;
    std::shared_ptr<brpc::RtmpAudioMessage> _aac_seq_header;
    // Save sent metadata/headers to avoid resending same stuff again.
    std::shared_ptr<brpc::RtmpMetaData> _sent_metadata;
    std::shared_ptr<brpc::RtmpVideoMessage> _sent_avc_seq_header;
    std::shared_ptr<brpc::RtmpAudioMessage> _sent_aac_seq_header;
    
    // used to control timestamp in metadata
    bool _reset_base_timestamp;
    uint32_t _base_timestamp;
    uint32_t _last_timestamp;
};

Player::Player(Publisher* publisher)
    : _publisher(publisher) 
    , _frame_queue()
    , _reset_base_timestamp(true)
    , _base_timestamp(0)
    , _last_timestamp(0) {
    _cursor = new DefaultCursor(&_frame_queue, FrameIndicator());
}
      
Player::~Player() {}

void Player::OnMetaData(brpc::RtmpMetaData* metadata, const butil::StringPiece& name) {
    _metadata.reset(new brpc::RtmpMetaData(*metadata));
    // cyberplayer treats the stream as recorded when `duration' is
    // present and sends a seek to the server and makes the player
    // buffering(black-screen). Remove the field to prevent this
    // from happening.
    _metadata->data.Remove("duration");
}

void Player::OnAudioMessage(brpc::RtmpAudioMessage* msg) {
    if (msg->IsAACSequenceHeader()) {
        _aac_seq_header.reset(new brpc::RtmpAudioMessage(*msg));
        _aac_seq_header->timestamp = 0;
        return;
    }
    Frame frame(*msg);
    _frame_queue.push(frame);
    flush();
}

void Player::OnVideoMessage(brpc::RtmpVideoMessage* msg) {
    if (msg->IsAVCSequenceHeader()) {
        _avc_seq_header.reset(new brpc::RtmpVideoMessage(*msg));
        _avc_seq_header->timestamp = 0;
        return;
    }
    Frame frame(*msg);
    if (frame.is_keyframe()) {
        frame.metadata = _metadata;
        frame.avc_seq_header = _avc_seq_header;
        frame.aac_seq_header = _aac_seq_header;
    }
    _frame_queue.push(frame);
    flush();
}

void Player::flush() {
    Frame frame;
    while (_cursor->next(&frame, false)) {
        if (frame.metadata != NULL && 
            frame.metadata != _sent_metadata) {
            brpc::RtmpMetaData cp = *frame.metadata;
            if (_reset_base_timestamp) {
                _reset_base_timestamp = false;
                _base_timestamp = cp.timestamp - _last_timestamp;
            }
            cp.timestamp -= _base_timestamp;
            _last_timestamp = cp.timestamp;
            if (_publisher->SendMetaData(cp) != 0) {
                goto FAIL_TO_SEND;
            }
            _sent_metadata = frame.metadata;
            VLOG(99) << _publisher->remote_side() << ": Sent metadata{"
                     << cp.data << '}'
                     << " timestamp=" << cp.timestamp;
        }
        if (frame.avc_seq_header != NULL &&
            frame.avc_seq_header != _sent_avc_seq_header) {
            if (_publisher->SendVideoMessage(*frame.avc_seq_header) != 0) {
                goto FAIL_TO_SEND;
            }
            _sent_avc_seq_header = frame.avc_seq_header;
            VLOG(99) << _publisher->remote_side() << ": Sent avc_seq_header("
                     << _sent_avc_seq_header->data.size() << ')';
        }
        if (frame.aac_seq_header != NULL &&
            frame.aac_seq_header != _sent_aac_seq_header) {
            if (_publisher->SendAudioMessage(*frame.aac_seq_header) != 0) {
                goto FAIL_TO_SEND;
            }
            _sent_aac_seq_header = frame.aac_seq_header;
            VLOG(99) << _publisher->remote_side() << ": Sent aac_seq_header("
                     << _sent_aac_seq_header->data.size() << ')';
        }
        if (frame.type == FRAME_VIDEO) {
            brpc::RtmpVideoMessage msg;
            CHECK(frame.swap_as(msg));
            if (_publisher->SendVideoMessage(msg) != 0) {
                goto FAIL_TO_SEND;
            }
        } else if (frame.type == FRAME_AUDIO) {
            brpc::RtmpAudioMessage msg;
            CHECK(frame.swap_as(msg));
            if (_publisher->SendAudioMessage(msg) != 0) {
                goto FAIL_TO_SEND;
            }
        } else {
            LOG(ERROR) << "Unknown frame_type=" << frame.type;
        }
        continue;
        
    FAIL_TO_SEND:
        if (errno == brpc::ERTMPPUBLISHABLE) {
            _cursor->reset();
            _sent_metadata.reset();
            _sent_avc_seq_header.reset();
            _sent_aac_seq_header.reset();
            _reset_base_timestamp = true;
            _base_timestamp = 0;
            // _last_timestamp must retain
            continue;
        } else {
            _cursor->backup();
            break;
        }
    }
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    butil::StringPiece host;
    butil::StringPiece port;
    butil::StringPiece vhost;
    butil::StringPiece pull_app;
    butil::StringPiece streams;
    brpc::ParseRtmpURL(FLAGS_streams, &host, &vhost, &port,
                             &pull_app, &streams);
    butil::StringPiece queries;
    streams = brpc::RemoveQueryStrings(streams, &queries);
    for (brpc::QuerySplitter qs(queries); qs; ++qs) {
        if (qs.key() == "vhost") {
            vhost = qs.value();
            break;
        }
    }
    std::string pull_addr;
    if (host.empty() || host == "localhost") {
        pull_addr = butil::my_hostname();
    } else {
        pull_addr = host.as_string();
    }
    if (port.empty()) {
        port = "1935";
    }
    pull_addr.push_back(':');
    pull_addr.append(port.data(), port.size());
    
    std::string push_addr = FLAGS_push_to;
    if (push_addr.empty()) {
        push_addr = butil::string_printf("%s:8079", butil::my_hostname());
    } else if (push_addr.find(':') == std::string::npos) {
        push_addr += ":1935";
    }

    brpc::RtmpClientOptions rtmp_opt;
    std::string pull_app_str = pull_app.as_string();
    rtmp_opt.app = pull_app_str;
    rtmp_opt.tcUrl = "rtmp://" + pull_addr + "/" + pull_app_str;
    brpc::RtmpClient play_client;
    if (play_client.Init(pull_addr.c_str(), rtmp_opt) != 0) {
        LOG(ERROR) << "Fail to init play_client";
        return -1;
    }
    if (FLAGS_push_app.empty()) {
        FLAGS_push_app = pull_app_str;
    }
    rtmp_opt.app = FLAGS_push_app;
    rtmp_opt.tcUrl = "rtmp://";
    if (FLAGS_push_vhost.empty()) {
        rtmp_opt.tcUrl += push_addr;
    } else {
        rtmp_opt.tcUrl += FLAGS_push_vhost;
    }
    rtmp_opt.tcUrl.push_back('/');
    rtmp_opt.tcUrl += FLAGS_push_app;
    brpc::RtmpClient publish_client;
    if (publish_client.Init(push_addr.c_str(), rtmp_opt) != 0) {
        LOG(ERROR) << "Fail to init publish_client";
        return -1;
    }

    if (FLAGS_dummy_port > 0) {
        brpc::StartDummyServerAt(FLAGS_dummy_port);
    }

    std::vector<Publisher*> publishers;
    std::vector<Player*> players;
    butil::StringMultiSplitter sp(streams.begin(), streams.end(), "\n\t ,");
    for (; sp; ++sp) {
        butil::StringPiece play_name(sp.field(), sp.length());
        butil::StringPiece publish_name = play_name;
        const size_t colon_pos = play_name.find(':');
        if (colon_pos != butil::StringPiece::npos) {
            publish_name = play_name.substr(colon_pos + 1);
            play_name.remove_suffix(play_name.size() - colon_pos);
        }
        Publisher* publisher = new Publisher;
        brpc::RtmpRetryingClientStreamOptions pub_opt;
        pub_opt.retry_interval_ms = FLAGS_retry_interval_ms;
        pub_opt.share_connection = FLAGS_push_share_conn;
        if (!FLAGS_push_stream.empty()) {
            pub_opt.publish_name = FLAGS_push_stream;
        } else {
            pub_opt.publish_name = publish_name.as_string();
        }
        pub_opt.publish_type = brpc::RTMP_PUBLISH_LIVE;
        pub_opt.quit_when_no_data_ever = false;
        brpc::SubStreamCreator *sc = new RtmpSubStreamCreator(&publish_client);
        publisher->Init(sc, pub_opt);
        publishers.push_back(publisher);
        
        Player* player = new Player(publisher);
        brpc::RtmpRetryingClientStreamOptions play_opt;
        play_opt.retry_interval_ms = FLAGS_retry_interval_ms;
        play_opt.share_connection = FLAGS_pull_share_conn;
        play_opt.play_name = play_name.as_string();
        play_opt.quit_when_no_data_ever = false;
        sc = new RtmpSubStreamCreator(&play_client);
        player->Init(sc, play_opt);
        
        players.push_back(player);
    }
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }
    LOG(INFO) << "pusher is going to quit";
    for (size_t i = 0; i < players.size(); ++i) {
        players[i]->Destroy();
    }
    players.clear();
    for (size_t i = 0; i < publishers.size(); ++i) {
        publishers[i]->Destroy();
    }
    publishers.clear();
    return 0;
}
