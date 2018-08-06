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

#include "butil/fast_rand.h"
#include <algorithm>
#include <gflags/gflags.h>
#include "butil/third_party/murmurhash3/murmurhash3.h"
#include "bthread/bthread.h"
#include "util.h"
#include "shared_map.h"

DEFINE_int64(disable_server_duration, 5, "Upstream server would be disabled for"
        "such seconds if we can't pull or push streams to this server");

std::string make_key(const butil::StringPiece& vhost,
                     const butil::StringPiece& app,
                     const butil::StringPiece& stream_name) {
    std::string key;
    key.reserve(vhost.size() + app.size() + stream_name.size() + 3);
    key.append(vhost.data(), vhost.size());
    key.push_back('/');
    key.append(app.data(), app.size());
    key.push_back('/');
    key.append(stream_name.data(), stream_name.size());
    return key;
}
static auto randchar = []() -> char {
    const char charset[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[butil::fast_rand_less_than(max_index)];
};

void generate_random_string(char* str, int size) {
    std::generate_n(str, size, randchar);
}

void generate_random_string(std::string& str) {
    std::generate_n(str.begin(), str.end() - str.begin(), randchar);
}

void generate_fake_header(brpc::RtmpAVCMessage& avc_msg,
                          brpc::RtmpAACMessage& aac_msg,
                          int64_t timestamp_ms,
                          bool is_first,
                          int64_t video_cyc_count,
                          int64_t gop_size) {
    avc_msg.timestamp = timestamp_ms;
    avc_msg.composition_time = 0;
    if (is_first) {
        avc_msg.packet_type = brpc::FLV_AVC_PACKET_SEQUENCE_HEADER;
        avc_msg.frame_type = brpc::FLV_VIDEO_FRAME_KEYFRAME;
    } else {
        avc_msg.packet_type = brpc::FLV_AVC_PACKET_NALU;
        if (video_cyc_count % gop_size == 0) {
            avc_msg.frame_type = brpc::FLV_VIDEO_FRAME_KEYFRAME;
        } else {
            avc_msg.frame_type = brpc::FLV_VIDEO_FRAME_INTERFRAME;
        }
    }

    aac_msg.timestamp = timestamp_ms;
    aac_msg.rate = brpc::FLV_SOUND_RATE_44100HZ;
    aac_msg.bits = brpc::FLV_SOUND_16BIT;
    aac_msg.type = brpc::FLV_SOUND_STEREO;
    if (is_first) {
        aac_msg.packet_type = brpc::FLV_AAC_PACKET_SEQUENCE_HEADER;
    } else {
        aac_msg.packet_type = brpc::FLV_AAC_PACKET_RAW;
    }
}
        
// ===== RtmpSubStream =====

void RtmpSubStream::OnFirstMessage() {
    _message_handler->OnPlayable();
}

void RtmpSubStream::OnMetaData(brpc::RtmpMetaData* obj, const butil::StringPiece& name) {
    _message_handler->OnMetaData(obj, name);
}

void RtmpSubStream::OnSharedObjectMessage(brpc::RtmpSharedObjectMessage* msg) {
    _message_handler->OnSharedObjectMessage(msg);
}

void RtmpSubStream::OnAudioMessage(brpc::RtmpAudioMessage* msg) {
    _has_data_ever.exchange(true, butil::memory_order_relaxed);
    _message_handler->OnAudioMessage(msg);
}

void RtmpSubStream::OnVideoMessage(brpc::RtmpVideoMessage* msg) {
    _has_data_ever.exchange(true, butil::memory_order_relaxed);
    _message_handler->OnVideoMessage(msg);
}
struct DisableServerKey {
    std::string server_addr;
    std::string key;
};
struct DisableServerInfo : public brpc::SharedObject {
    DisableServerInfo(const DisableServerKey& key)
        : add_time(0) {}
    int64_t add_time; 
};
struct HashDisableServerKey {
    size_t operator()(const DisableServerKey& key) const {
        uint64_t result[2];
        butil::MurmurHash3_x64_128_Context ctx;
        butil::MurmurHash3_x64_128_Init(&ctx, 0);
        butil::MurmurHash3_x64_128_Update(&ctx, key.server_addr.data(),
                                               key.server_addr.size());
        butil::MurmurHash3_x64_128_Update(&ctx, key.key.data(), key.key.size());
        butil::MurmurHash3_x64_128_Final(&result, &ctx);
        return result[0];
    }
};

typedef SharedMap<DisableServerKey, DisableServerInfo, HashDisableServerKey> DisableServer;
static DisableServer g_disable_server;
struct IsDisableServerReleased {
    bool operator()(const DisableServerInfo& info) const {
        return butil::gettimeofday_s() >= info.add_time + FLAGS_disable_server_duration;
    }
};
bool operator==(const DisableServerKey& lhs, const DisableServerKey& rhs) {
    return lhs.server_addr == rhs.server_addr && lhs.key == rhs.key;
}
static void* remove_disable_server(void*) {
    IsDisableServerReleased filter;
    std::vector<std::pair<DisableServerKey, butil::intrusive_ptr<DisableServerInfo> > > removals;
    while (true) {
        removals.clear();
        g_disable_server.remove_by(filter, &removals);
        bthread_usleep(1000000/*1s*/);
    }
    return NULL;
}
static pthread_once_t g_remove_disable_server_thread_once = PTHREAD_ONCE_INIT;
static void remove_disable_server_thread(void) {
    bthread_t th;
    CHECK_EQ(0, bthread_start_background(&th, NULL, remove_disable_server, NULL));
}

bool is_server_usable(const std::string& server_addr, const std::string& key) {
    butil::intrusive_ptr<DisableServerInfo> value;
    return !g_disable_server.get(DisableServerKey{server_addr, key}, &value);
}

void RtmpSubStream::OnStop() {
    if (!_has_data_ever.load(butil::memory_order_relaxed) &&
        !_server_addr.empty() && !_key.empty() && _internal_service) {
        butil::intrusive_ptr<DisableServerInfo> value;
        g_disable_server.get_or_new(DisableServerKey{_server_addr, _key}, &value);
        value->add_time = butil::gettimeofday_s();
    }
    _message_handler->OnSubStreamStop(this);
}

// ===== RtmpSubStreamCreator =====

RtmpSubStreamCreator::RtmpSubStreamCreator(
    RtmpClientSelector* client_selector,
    bool internal_service)
    : _client_selector(client_selector)
    , _client(NULL)
    , _internal_service(internal_service) {
    pthread_once(&g_remove_disable_server_thread_once,
                 remove_disable_server_thread);
}

RtmpSubStreamCreator::RtmpSubStreamCreator(const brpc::RtmpClient* client)
    : _client_selector(NULL)
    , _client(client)
    , _internal_service(false) {
    pthread_once(&g_remove_disable_server_thread_once,
                 remove_disable_server_thread);
}

RtmpSubStreamCreator::~RtmpSubStreamCreator() {
    if (_client_selector) {
        delete _client_selector; 
    }
}
 
void RtmpSubStreamCreator::NewSubStream(brpc::RtmpMessageHandler* message_handler,
                                        butil::intrusive_ptr<brpc::RtmpStreamBase>* sub_stream) {
    if (sub_stream) { 
        (*sub_stream).reset(new RtmpSubStream(message_handler, _internal_service));
    }
    return;
}

void OnGetRtmpClient::Run(const brpc::RtmpClient* client) {
    std::unique_ptr<OnGetRtmpClient> delete_self(this);
    if (client == NULL) {
        sub_stream->Destroy();
    } else {
        sub_stream->server_addr() = server_addr;
        sub_stream->key() = key;
        sub_stream->Init(client, options);
    }
}

void RtmpSubStreamCreator::LaunchSubStream(
    brpc::RtmpStreamBase* sub_stream, 
    brpc::RtmpRetryingClientStreamOptions* options) {
    brpc::RtmpClientStreamOptions client_options = *options;
    if (_client_selector) {
        OnGetRtmpClient* done = new OnGetRtmpClient;
        done->sub_stream.reset(dynamic_cast<RtmpSubStream*>(sub_stream));
        done->options = client_options;
        _client_selector->StartGettingRtmpClient(done);
    } else {
        dynamic_cast<RtmpSubStream*>(sub_stream)->Init(_client, client_options);
    }
}
// ===== RtmpSubStreamCreator =====

void build_uri_without_queries(const brpc::URI& uri, std::ostream& os) {
    if (!uri.host().empty()) {
        if (!uri.schema().empty()) {
            os << uri.schema() << "://";
        } else {
            os << "http://";
        }
        os << uri.host();
        if (uri.port() > 0) {
            os << ':' << uri.port();
        }
    }
    if (uri.path().empty()) {
        os << '/';
    } else {
        os << uri.path();
    }
}

butil::StringPiece get_value_from_query_by_key(const butil::StringPiece& queries,
                                              const butil::StringPiece& key) {
    for (brpc::QuerySplitter qs(queries); qs; ++qs) {
        if (qs.key() == key) {
            return qs.value();
        }
    }
    return butil::StringPiece();
}

// Return true if stream ends with suffix in suffixes
bool longest_suffix_match(const std::vector<std::string>& suffixes,
                          const butil::StringPiece& stream,
                          butil::StringPiece* result) {
    unsigned int maxlength = 0;
    for (auto it = suffixes.cbegin(); it != suffixes.cend(); ++it) {
        if (stream.length() >= (*it).length() &&
            stream.rfind(*it) == stream.length() - (*it).length() && 
            (*it).length() > maxlength) {
            maxlength = (*it).length();
        }
    }
    if (result) {
        *result =  stream.substr(0, stream.length() - maxlength);
    }
    return maxlength != 0;
}

bool is_outtest_cdn(const butil::StringPiece& queries) {
    return get_idcpassed_from_queries(queries).empty();
}

butil::StringPiece get_idcpassed_from_queries(const butil::StringPiece& queries) {
    for (brpc::QuerySplitter qs(queries); qs; ++qs) {
        if (qs.key() == IDCPASSED) {
            return qs.value();
        }
    }
    return butil::StringPiece();
}

int decode_url(const butil::StringPiece& encoded_url, std::string* decoded_url) {
    if (!decoded_url) {
        return -1;
    }
    decoded_url->clear();
    size_t length = encoded_url.length();
    for (size_t i = 0; i < length; i++) {
        switch (encoded_url[i]) {
        case '+':
            decoded_url->push_back(' ');
            break;
        case '%':
            {
                std::string decoded_char;
                if (i + 2 >= length || hex_string_to_chars(encoded_url.substr(i+1, 2).as_string(), &decoded_char) != 0) {
                    return -1;
                }
                decoded_url->push_back(*decoded_char.c_str());
                i += 2;
                break;
            }
        default:
            decoded_url->push_back(encoded_url[i]);
        }
    }
    return 0;
}

int hex_to_dec(const unsigned char hex_in, int* dec_out) {
    if (((hex_in >= '0') && (hex_in <= '9'))) {
        *dec_out = hex_in - '0';
        return 0;
    }
    if (((hex_in >= 'a') && (hex_in <= 'f'))) {
        *dec_out = hex_in - 'a' + 10;
        return 0;
    }
    if (((hex_in >= 'A') && (hex_in <= 'F'))) {
        *dec_out = hex_in - 'A' + 10;
        return 0;
    }
    return -1;
}

int hex_string_to_chars(const std::string& hex_string_in, std::string* chars_out) {
    if (!chars_out) {
        return -1;
    }
    std::string chars;
    for (unsigned int i = 0; i < hex_string_in.size(); i += 2){
        int high = 0;
        if (hex_to_dec(hex_string_in[i], &high) != 0) {
            LOG(ERROR) << "Fail to convert hex char to int";
            return -1;
        }
        int low = 0;
        if ((i + 1 >= hex_string_in.size()) || (hex_to_dec(hex_string_in[i+1], &low) != 0)) {
            LOG(ERROR) << "Hex string error or fail to convert hex char to int";
            return -1;
        }
        chars.push_back(char(high << 4 | low));
    }
    chars_out->assign(chars.data(), chars.size());
    return 0;
}

void chars_to_hex_string(const std::string& chars_in, std::string* hex_string_out) {
    if (!hex_string_out) {
        return;
    }
    std::string hex_string;
    const char hex_chars[16] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    for (unsigned int i = 0; i < chars_in.size(); ++i){
        const char byte = chars_in[i];
        hex_string.push_back(hex_chars[(byte >> 4) & 0x0F]);
        hex_string.push_back(hex_chars[(byte & 0x0F)]);
    }
    *hex_string_out = hex_string;
}

bool wildcard_match(const std::string& pattern, const std::string& src) {
    // Suppose p and s indicate the position to match for pattern and src,
    // 1. if the char of p is equal the char of s, continue;
    // 2. if the char of p is '?', continue;
    // 3. if the char of p is '*', continue;
    // 4. if the above 1.2,3 do not work, but pattern has a '*' before p, continue with the char after '*'.
    // Following shows the match is successful,
    // 5. if src has come to end, leaving pattern with continuous '*';
    // 6. if src and pattern have both come to end;
    uint64_t pattern_pos = 0;
    uint64_t src_pos = 0;
    int64_t pattern_star_pos = -1;
    int64_t src_match_star_pos = 0;
    while (src_pos < src.length()) {
        if (pattern_pos < pattern.length() && pattern[pattern_pos] == '*') {
            pattern_star_pos = pattern_pos;
            pattern_pos++;
            src_match_star_pos = src_pos;
        } else if (pattern_pos < pattern.length() && (src[src_pos] == pattern[pattern_pos] || 
                   pattern[pattern_pos] == '?')) {
            pattern_pos++;
            src_pos++;
        } else if (pattern_star_pos != -1) {
            pattern_pos = pattern_star_pos + 1;
            src_match_star_pos++;
            src_pos = src_match_star_pos;
        } else {
            return false;
        }
    }

    while (pattern_pos < pattern.length() && pattern[pattern_pos] == '*') {
        pattern_pos++;
    }
    
    return pattern_pos == pattern.length();
}

void append_host_to_http_header(const butil::StringPiece& host,
                                brpc::HttpHeader* http_header) {
    butil::StringPiece modified_host = host;
    butil::StringPiece::size_type pos = modified_host.find("://");
    if (pos != butil::StringPiece::npos) {
        modified_host.remove_prefix(pos + 3 /* :// */);
    }
    http_header->SetHeader("Host", modified_host.as_string());
}
