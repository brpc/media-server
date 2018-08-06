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

#ifndef MEDIA_SERVER_UTIL_H
#define MEDIA_SERVER_UTIL_H

#include "brpc/rtmp.h"

// The key uniquely identifies a stream inside media-server.
std::string make_key(const butil::StringPiece& vhost,
                     const butil::StringPiece& app,
                     const butil::StringPiece& stream_name);

void generate_random_string(char* str, int size);
void generate_random_string(std::string& str);

int decode_url(const butil::StringPiece& encoded_url, std::string* decoded_url);

// A utility to ease construction of jsons.
class JsonAppender {
public:
    explicit JsonAppender(std::ostringstream& os) : _n(0), _os(&os) {}
    ~JsonAppender() { close(); }
    void close() {
        if (_os) {
            if (_n == 0) {
                *_os << '{';
            }
            *_os << '}';
            _os = NULL;
        }
    }
    void append(const butil::StringPiece& name, const butil::StringPiece& value)
    { *_os << (_n++ ? ',' : '{') << '"' << name << "\":\"" << value << '"'; }
    void append(const butil::StringPiece& name, int64_t value)
    { *_os << (_n++ ? ',' : '{') << '"' << name << "\":" << value; }
    void append_with_open_value(const butil::StringPiece& name)
    { *_os << (_n++ ? ',' : '{') << '"' << name << "\":"; }
private:
    size_t _n;
    std::ostringstream* _os;
};

void generate_fake_header(brpc::RtmpAVCMessage& avc_msg,
                          brpc::RtmpAACMessage& aac_msg,
                          int64_t timestamp_ms,
                          bool is_first,
                          int64_t video_cyc_count,
                          int64_t gop_size);

inline uint32_t ReadLittleEndian4Bytes(const void* void_buf) {
    uint32_t ret = 0;
    char* p = (char*)&ret;
    const char* buf = (const char*)void_buf;
    p[3] = buf[3];
    p[2] = buf[2];
    p[1] = buf[1];
    p[0] = buf[0];
    return ret;
}

inline uint16_t ReadLittleEndian2Bytes(const void* void_buf) {
    uint16_t ret = 0;
    char* p = (char*)&ret;
    const char* buf = (const char*)void_buf;
    p[1] = buf[1];
    p[0] = buf[0];
    return ret;
}

inline void WriteLittleEndian4Bytes(uint32_t data, void* void_buf) {
    char* buf = (char*)void_buf;
    char* p = (char *)&data;
    buf[3] = p[3];
    buf[2] = p[2];
    buf[1] = p[1];
    buf[0] = p[0];
}

inline void WriteLittleEndian2Bytes(uint16_t data, void* void_buf) {
    char* buf = (char*)void_buf;
    char* p = (char *)&data;
    buf[1] = p[1];
    buf[0] = p[0];
}

class RtmpSubStream : public brpc::RtmpClientStream {
public:
    explicit RtmpSubStream(brpc::RtmpMessageHandler* mh, bool internal_service)
        : _message_handler(mh)
        , _has_data_ever(false)
        , _internal_service(internal_service) {}
    // @RtmpStreamBase
    void OnMetaData(brpc::RtmpMetaData*, const butil::StringPiece&);
    void OnSharedObjectMessage(brpc::RtmpSharedObjectMessage* msg);
    void OnAudioMessage(brpc::RtmpAudioMessage* msg);
    void OnVideoMessage(brpc::RtmpVideoMessage* msg);
    void OnFirstMessage();
    void OnStop();

    std::string& server_addr() { return _server_addr; }
    std::string& key() { return _key; }
private:
    std::unique_ptr<brpc::RtmpMessageHandler> _message_handler;
    std::string _server_addr;
    std::string _key;
    butil::atomic<bool> _has_data_ever;
    bool _internal_service;
};

class OnGetRtmpClient {
public:
    void Run(const brpc::RtmpClient*);
public:
    butil::intrusive_ptr<RtmpSubStream> sub_stream;
    brpc::RtmpClientStreamOptions options;
    std::string server_addr;
    std::string key;
};

// Base class for choosing a RtmpClient for RtmpRetryingClientStream dynamically.
class RtmpClientSelector {
public:
    virtual ~RtmpClientSelector() {}
    // Start getting a RtmpClient and call done->Run(client) when the client
    // is got and ready.
    virtual void StartGettingRtmpClient(OnGetRtmpClient* done) = 0;
};

class RtmpSubStreamCreator : public brpc::SubStreamCreator {
public:
    RtmpSubStreamCreator(RtmpClientSelector* client_selector, bool internal_service);
    RtmpSubStreamCreator(const brpc::RtmpClient* client);
    ~RtmpSubStreamCreator();
    
    // @SubStreamCreator
    void NewSubStream(brpc::RtmpMessageHandler* message_handler,
                      butil::intrusive_ptr<brpc::RtmpStreamBase>* sub_stream);
    void LaunchSubStream(brpc::RtmpStreamBase* sub_stream,
                         brpc::RtmpRetryingClientStreamOptions* options);

private:
    RtmpClientSelector* _client_selector;
    const brpc::RtmpClient* _client;
    bool _internal_service;
};

bool is_server_usable(const std::string& server_addr, const std::string& key);

void build_uri_without_queries(const brpc::URI& uri, std::ostream& os);

// If key=value is not found in query, then an empty butil::StringPiece is returned.
butil::StringPiece get_value_from_query_by_key(const butil::StringPiece& queries,
                                              const butil::StringPiece& key);

bool longest_suffix_match(const std::vector<std::string>& suffixes,
                          const butil::StringPiece& stream,
                          butil::StringPiece* result);

const char* const IDCPASSED = "idcpassed";
const char* const RELAY = "relay";

bool has_relay_in_queries(const butil::StringPiece& queries);
butil::StringPiece get_idcpassed_from_queries(const butil::StringPiece& queries);
bool is_outtest_cdn(const butil::StringPiece& queries);

int hex_to_dec(const unsigned char hex_in, int* dec_out);

int hex_string_to_chars(const std::string& hex_string_in, std::string* chars_out);

void chars_to_hex_string(const std::string& chars_in, std::string* hex_string_out);

bool wildcard_match(const std::string& wildcard, const std::string& src);

void append_host_to_http_header(const butil::StringPiece& host,
                                brpc::HttpHeader* http_header);
#endif // MEDIA_SERVER_UTIL_H
