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
#include "brpc/channel.h"
#include "butil/logging.h"
#include "rtmp_forward_service.h"
#include "http_streaming_service.h"

DEFINE_int32(port, 8079, "Listening port of this server");
DEFINE_int32(internal_port, -1, "Only provide builtin services at this port, "
             "which is only accessible from internal network");

DEFINE_string(proxy_to, "", "Proxy streams to the server(or servers)");
DEFINE_string(proxy_lb, "", "Load balancing algorithm for choosing one"
              " server from -proxy_to");
DEFINE_bool(share_play_connection, true,
            "Multiple play streams over one connection");
DEFINE_bool(share_publish_connection, true,
            "Multiple publish streams over one connection");

DEFINE_string(cdn_merge_to, "", "Proxy streams to servers specified by this "
              "flag, which then proxies the streams to -proxy_to. A technique "
              "used in CDN to reduce traffic to source sites.");
DEFINE_string(cdn_merge_lb, "", "Load balancing algorithm for choosing one"
              " server from -cdn_merge_to");

DEFINE_string(cdn_probe_file, "baidu-cdn-probe.gif", "use for cdn national monitoring network");
DEFINE_int32(server_idle_timeout, -1, "connections without data transmission"
             " for so many seconds will be closed");

DEFINE_bool(https, false, "Indicate whether https is supported."
            "Flags like certificate and private_key are used only when this flag is set to true");
DEFINE_string(certificate, "./cert.pem", "Certificate file path to enable SSL");
DEFINE_string(private_key, "./key.pem", "Private key file path to enable SSL");

bool at_cdn() { return !FLAGS_cdn_merge_to.empty(); }

namespace brpc {
DECLARE_int32(defer_close_second);
}

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    // We create ad-hoc http channels for pulling TS, make sure the connection
    // is not closed immediately for performance considerations.
    if (brpc::FLAGS_defer_close_second < 10) {
        brpc::FLAGS_defer_close_second = 10;
    }

    RtmpForwardService* rtmp_merge_service = NULL;
    int merge_port = -1;
    if (!FLAGS_cdn_merge_to.empty()) {
        size_t colon_pos = FLAGS_cdn_merge_to.find_last_of(':');
        if (colon_pos == std::string::npos) {
            LOG(ERROR) << "-cdn_merge_to=" << FLAGS_cdn_merge_to
                       << " does not contain a port";
            return -1;
        }
        merge_port = strtol(FLAGS_cdn_merge_to.c_str() + colon_pos + 1,
                            NULL, 10);
        if (merge_port < 0) {
            LOG(ERROR) << "Invalid port=" << merge_port << " in -cdn_merge_to="
                       << FLAGS_cdn_merge_to;
            return -1;
        }
        if (merge_port == FLAGS_port || merge_port == FLAGS_internal_port) {
            LOG(ERROR) << "port=" << merge_port << " in -cdn_merge_to="
                       << FLAGS_cdn_merge_to << " is conflict with -port"
                " or -internal_port";
            return -1;
        }
        rtmp_merge_service = new RtmpForwardService;
        RtmpForwardServiceOptions rtmp_merge_opt;
        rtmp_merge_opt.port = merge_port;
        rtmp_merge_opt.proxy_to = FLAGS_proxy_to;
        rtmp_merge_opt.proxy_lb = FLAGS_proxy_lb;
        rtmp_merge_opt.share_play_connection = FLAGS_share_play_connection;
        rtmp_merge_opt.internal_service = true;
        if (rtmp_merge_service->init(rtmp_merge_opt) != 0) {
            LOG(ERROR) << "Fail to init rtmp_merge_service";
            return -1;
        }
    }

    // NOTE: many code running in different threads relies on rtmp_service, we
    // intentionally don't delete rtmp_service to make it always valid.
    RtmpForwardService* rtmp_service = new RtmpForwardService;
    RtmpForwardServiceOptions rtmp_opt;
    rtmp_opt.port = FLAGS_port;
    rtmp_opt.internal_port = FLAGS_internal_port;
    // Different from play, publish is not consistently hashed, thus the
    // share_connection flag is always applied to rtmp_service rather than
    // rtmp_merge_service.
    rtmp_opt.share_publish_connection = FLAGS_share_publish_connection;
    if (rtmp_merge_service == NULL) {
        rtmp_opt.proxy_to = FLAGS_proxy_to;
        rtmp_opt.proxy_lb = FLAGS_proxy_lb;
        rtmp_opt.share_play_connection = FLAGS_share_play_connection;
    } else {
        rtmp_opt.proxy_to = FLAGS_cdn_merge_to;
        rtmp_opt.proxy_lb = FLAGS_cdn_merge_lb;
    }
    if (rtmp_service->init(rtmp_opt) != 0) {
        LOG(ERROR) << "Fail to init rtmp_service";
        return -1;
    }

    HttpStreamingServiceOptions http_opt;
    http_opt.proxy_hls = (rtmp_merge_service != NULL);
    HttpStreamingServiceImpl http_streaming_service(rtmp_service, http_opt);
    MonitoringServiceImpl monitoring_service(rtmp_service);
    brpc::Server server;
    server.set_version("media_server");
    std::string mapping_tmp =
        "*.flv => stream_flv, "
        "*.ts => stream_ts, "
        "*.dynamic.m3u8 => get_media_playlist, "
        "*.m3u8 => get_master_playlist, "
        "crossdomain.xml => get_crossdomain_xml, "
        "play_hls => play_hls, "
        "get_hls_min => get_hls_min, "
         + FLAGS_cdn_probe_file + " => get_cdn_probe";
    const char* const HTTP_SERVICE_RESTFUL_MAPPINGS = mapping_tmp.c_str();
    if (server.AddService(&http_streaming_service,
                          brpc::SERVER_DOESNT_OWN_SERVICE,
                          HTTP_SERVICE_RESTFUL_MAPPINGS) != 0) {
        LOG(ERROR) << "Fail to add http_streaming_service";
        return -1;
    }
    const char* const MONITORING_SERVICE_RESTFUL_MAPPINGS =
        "/media_server/* => monitor, "
        "/players/* => players, "
        "/urls/* => urls";
    if (server.AddService(&monitoring_service,
                          brpc::SERVER_DOESNT_OWN_SERVICE,
                          MONITORING_SERVICE_RESTFUL_MAPPINGS) != 0) {
        LOG(ERROR) << "Fail to add monitoring_service";
        return -1;
    }

    brpc::ServerOptions server_opt;
    if (FLAGS_https) {
        server_opt.mutable_ssl_options()->default_cert.certificate = FLAGS_certificate;
        server_opt.mutable_ssl_options()->default_cert.private_key = FLAGS_private_key;
        server_opt.mutable_ssl_options()->strict_sni= false;
    }
    server_opt.rtmp_service = rtmp_service;
    server_opt.idle_timeout_sec = FLAGS_server_idle_timeout;
    server_opt.internal_port = rtmp_opt.internal_port;
    if (server.Start(rtmp_opt.port, &server_opt) != 0) {
        LOG(ERROR) << "Fail to start media_server";
        return -1;
    }

    if (rtmp_merge_service != NULL) {
        HttpStreamingServiceOptions http_opt;
        http_opt.proxy_hls = false;
        HttpStreamingServiceImpl http_merge_service(rtmp_merge_service, 
                                                    http_opt);
        MonitoringServiceImpl monitoring_merge_service(rtmp_merge_service);
        brpc::Server merge_server;
        merge_server.set_version("media_server(cdn-merge)");
        if (merge_server.AddService(&http_merge_service,
                                    brpc::SERVER_DOESNT_OWN_SERVICE,
                                    HTTP_SERVICE_RESTFUL_MAPPINGS) != 0) {
            LOG(ERROR) << "Fail to add http_merge_service";
            return -1;
        }
        if (merge_server.AddService(&monitoring_merge_service,
                                    brpc::SERVER_DOESNT_OWN_SERVICE,
                                    MONITORING_SERVICE_RESTFUL_MAPPINGS) != 0) {
            LOG(ERROR) << "Fail to add monitoring_merge_service";
            return -1;
        }

        brpc::ServerOptions server_opt;
        server_opt.rtmp_service = rtmp_merge_service;
        server_opt.idle_timeout_sec = FLAGS_server_idle_timeout;
        if (merge_server.Start(merge_port, &server_opt) != 0) {
            LOG(ERROR) << "Fail to start media_server(cdn-merge)";
            return -1;
        }
        // NOTE: quit server before merge_server otherwise a lot of pulling
        // warnings will be reported.
        server.RunUntilAskedToQuit();
        merge_server.RunUntilAskedToQuit();
    } else {
        server.RunUntilAskedToQuit();
    }
    return 0;
}
