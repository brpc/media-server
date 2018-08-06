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

#ifndef MEDIA_SERVER_HTTP_STREAMING_SERVICE_H
#define MEDIA_SERVER_HTTP_STREAMING_SERVICE_H

#include "rtmp_forward_service.h"
#include "ts_queue.h"
#include <ifaddrs.h>
#include <boost/shared_ptr.hpp>

// Convert RTMP stream to progressive downloading of FLV files.
class FlvDownloader : public brpc::RtmpStreamBase {
public:
    FlvDownloader(brpc::ProgressiveAttachment* flv_pa,
                  RtmpForwardService* forward_service,
                  const std::string& play_key,
                  const std::string& charge_key,
                  bool audio_enabled,
                  bool video_enabled);
    ~FlvDownloader();

    // @RtmpStreamBase
    int SendMetaData(const brpc::RtmpMetaData& metadata, const butil::StringPiece& name);
    int SendAudioMessage(const brpc::RtmpAudioMessage& msg);
    int SendVideoMessage(const brpc::RtmpVideoMessage& msg);
    butil::EndPoint remote_side() const;
    butil::EndPoint local_side() const;

    // @Describable
    void Describe(std::ostream& os, const brpc::DescribeOptions&) const;

    void print_event_log(EventLogAction action) {
        _event_log_manager.print_event_log(action, _charge_key, _sent_bytes, 0);
    }

private:
    int flush_or_revert(ssize_t size_on_failure = -1);
    
    int64_t _first_avc_seq_header_delay;
    int64_t _first_video_message_delay;
    int64_t _first_aac_seq_header_delay;
    int64_t _first_audio_message_delay;
    bool _audio_enabled;
    bool _video_enabled;
    butil::IOBuf _flv_data;
    brpc::FlvWriter _flv_writer;
    butil::intrusive_ptr<brpc::ProgressiveAttachment> _flv_pa;
    size_t _sent_bytes;
    std::string _play_key;
    std::string _charge_key;
    EventLogManager _event_log_manager;
};

struct HttpStreamingServiceOptions {
    // With default options.
    HttpStreamingServiceOptions();

    // Proxy HLS requests(m3u8/ts) to servers specified by
    // RtmpForwardServiceOptions.proxy_to
    // Default: false
    bool proxy_hls;
};

// For FLV/HLS streaming.
class HttpStreamingServiceImpl : public HttpStreamingService {
public:
    HttpStreamingServiceImpl(RtmpForwardService* forward_service,
                             const HttpStreamingServiceOptions& opt);

    void stream_flv(google::protobuf::RpcController* cntl_base,
                    const HttpRequest*,
                    HttpResponse*,
                    google::protobuf::Closure* done);
    void stream_ts(google::protobuf::RpcController* cntl_base,
                   const HttpRequest*,
                   HttpResponse*,
                   google::protobuf::Closure* done);
    void get_media_playlist(google::protobuf::RpcController* cntl_base,
                            const HttpRequest*,
                            HttpResponse*,
                            google::protobuf::Closure* done);
    void get_master_playlist(google::protobuf::RpcController* cntl_base,
                             const HttpRequest*,
                             HttpResponse*,
                             google::protobuf::Closure* done);
    void get_crossdomain_xml(google::protobuf::RpcController* cntl_base,
                             const HttpRequest*,
                             HttpResponse*,
                             google::protobuf::Closure* done);
    void play_hls(google::protobuf::RpcController* cntl_base,
                  const HttpRequest*,
                  HttpResponse*,
                  google::protobuf::Closure* done);
    void get_hls_min(google::protobuf::RpcController* cntl_base,
                     const HttpRequest*,
                     HttpResponse*,
                     google::protobuf::Closure* done);
    void get_cdn_probe(google::protobuf::RpcController* cntl_base,
                       const HttpRequest*,
                       HttpResponse*,
                       google::protobuf::Closure* done);

    const RtmpForwardServiceOptions& rtmp_options() const
    { return _forward_service->options(); }

    const HttpStreamingServiceOptions& http_options() const
    { return _httpopt; }

private:
friend class ProxyHttp;
    RtmpForwardService* _forward_service;
    HttpStreamingServiceOptions _httpopt;
    std::unique_ptr<brpc::Channel> _proxy_http_channel;
    std::string _node_vip_with_port;
};

butil::ip_t get_host_public_ip();

// The handler for dealing with the response from upstream server in ProxyHttp. 
// By Implementing interface Handle, you can put everything you need in 
// client_cntl->response_attachment() into server_cntl->response_attachment, 
// or save some data into local storage.
class ResponseHandler {
public:
    virtual void Handle(const brpc::Controller& client_cntl,
                        brpc::Controller* server_cntl) = 0; 
};

class ControllerModifier {
public:
    virtual void modify(brpc::Controller* cntl) = 0;
};

class ProxyHttp : public OnGetTargetServer
                , public google::protobuf::Closure {
public:
    ProxyHttp(HttpStreamingServiceImpl* service,
              brpc::Controller* server_cntl,
              google::protobuf::Closure* server_done,
              const std::string& key,
              bool progressive_body);
    ~ProxyHttp();
    // @OnGetTargetServer
    void on_get_target_server(const butil::Status& status,
                              const char* server_addr,
                              const char* lb_algo);
    // @google::protobuf::Closure
    void Run();

    // Will not be NULL when the content is TS.
    butil::intrusive_ptr<TsEntry>& ts_entry() { return _ts_entry; }

    boost::shared_ptr<ControllerModifier>& controller_modifier() {
        return _controller_modifier;
    }
    boost::shared_ptr<ResponseHandler>& response_attachment_handler() {
        return _response_attachment_handler;
    }
    boost::shared_ptr<ResponseHandler>& failed_response_handler() {
        return _failed_response_handler;
    }

private:
    HttpStreamingServiceImpl* _service;
    brpc::Controller* _server_cntl;
    google::protobuf::Closure* _server_done; // called in dtor
    brpc::Controller _client_cntl;
    std::string _key;
    bool _progressive_body;
    butil::intrusive_ptr<TsEntry> _ts_entry;
    boost::shared_ptr<ControllerModifier> _controller_modifier;
    boost::shared_ptr<ResponseHandler> _response_attachment_handler;
    boost::shared_ptr<ResponseHandler> _failed_response_handler;
};

void remove_from_service(RtmpForwardService* forward_service,
                         std::string key,
                         brpc::RtmpStreamBase* stream);

#endif // MEDIA_SERVER_HTTP_STREAMING_SERVICE_H
