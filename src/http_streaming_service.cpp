v// Media Server - Host or proxy live multimedia streams.
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
#include "butil/memory/singleton_on_pthread_once.h"
#include "butil/md5.h"
#include "bthread/bthread.h"
#include "brpc/channel.h"
#include "brpc/callback.h"
#include "brpc/describable.h"
#include "brpc/builtin/common.h"
#include "brpc/policy/hasher.h"
#include <gperftools/malloc_extension.h>
#include "hls_min_js.h"
#include "ts_queue.h"
#include "http_streaming_service.h"
#include "rtmp_forward_service.h"
#include "event_log_manager.h"
#include "frame_queue.h"

DECLARE_int32(connect_timeout_ms);
DECLARE_int32(timeout_ms);
DECLARE_string(default_vhost);
DECLARE_int32(retry_interval_ms);

DEFINE_int64(get_media_playlist_times, 20, "the retry time that getting media playlist to avoid transcoded stream not ready problem");
DEFINE_bool(use_host_in_m3u8, true, "whether to use host in url in the response of m3u8");

static bvar::Adder<int64_t> g_flv_user_count("flv_user_count");

bool operator==(const TsEntryKey& lhs, const TsEntryKey& rhs) {
    return lhs.key == rhs.key && lhs.seq == rhs.seq &&
        lhs.started_with_keyframe == rhs.started_with_keyframe &&
        lhs.expiration_ms == rhs.expiration_ms;
}

FlvDownloader::FlvDownloader(brpc::ProgressiveAttachment* flv_pa,
                             RtmpForwardService* forward_service,
                             const std::string& play_key,
                             const std::string& charge_key,
                             bool audio_enabled,
                             bool video_enabled)
    : brpc::RtmpStreamBase(false)
    , _first_avc_seq_header_delay(0)
    , _first_video_message_delay(0)
    , _first_aac_seq_header_delay(0)
    , _first_audio_message_delay(0)
    , _audio_enabled(audio_enabled)
    , _video_enabled(video_enabled)
    , _flv_writer(&_flv_data)
    , _flv_pa(flv_pa)
    , _sent_bytes(0)
    , _play_key(play_key)
    , _charge_key(charge_key)
    , _event_log_manager(EVENT_LOG_FLV,
                         forward_service->options().internal_service,
                         _flv_pa? _flv_pa->remote_side(): butil::EndPoint()) {
    g_flv_user_count << 1;
}
    
FlvDownloader::~FlvDownloader() {
    if (!_charge_key.empty()) {
        print_event_log(EVENT_LOG_STOP_PLAY);
    }
    VLOG(99) << __FUNCTION__ << '(' << this << "), original_play_key=" << _charge_key
             << " remote_side=" << remote_side();
    g_flv_user_count << -1;
}

void FlvDownloader::Describe(std::ostream& os,
                             const brpc::DescribeOptions&) const {
    os << "vsh1=" << _first_avc_seq_header_delay / 1000.0
       << "ms v1=" << _first_video_message_delay / 1000.0
       << "ms ash1=" << _first_aac_seq_header_delay / 1000.0
       << "ms a1=" << _first_audio_message_delay / 1000.0
       << "ms";
}

int FlvDownloader::SendMetaData(const brpc::RtmpMetaData& metadata, const butil::StringPiece& name) {
    brpc::RtmpMetaData cp;
    const brpc::RtmpMetaData* written_metadata = NULL;
    if (_audio_enabled && _video_enabled) {
        written_metadata = &metadata;
    } else {
        cp = metadata;
        written_metadata = &cp;
        if (!_audio_enabled) {
            remove_audio_releated_fields(&cp.data);
        }
        if (!_video_enabled) {
            remove_video_releated_fields(&cp.data);
        }
    }
    const size_t old_size = _flv_data.size();
    butil::Status st = _flv_writer.Write(*written_metadata);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to write metadata into FLV, " << st;
        return -1;
    }
    return flush_or_revert(old_size);
}
int FlvDownloader::SendAudioMessage(const brpc::RtmpAudioMessage& msg) {
    if (!_audio_enabled) {
        return flush_or_revert();
    }
    if (msg.IsAACSequenceHeader()) {
        if (!_first_aac_seq_header_delay) {
            _first_aac_seq_header_delay = butil::gettimeofday_us() - create_realtime_us();
        }
    } else {
        if (!_first_audio_message_delay) {
            _first_audio_message_delay = butil::gettimeofday_us() - create_realtime_us();
        }
    }
    const size_t old_size = _flv_data.size();
    butil::Status st = _flv_writer.Write(msg);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to write audio message into FLV, " << st;
        return -1;
    }
    return flush_or_revert(old_size);
}
    
int FlvDownloader::SendVideoMessage(const brpc::RtmpVideoMessage& msg) {
    if (!_video_enabled) {
        return flush_or_revert();
    }
    if (msg.IsAVCSequenceHeader()) {
        if (!_first_avc_seq_header_delay) {
            _first_avc_seq_header_delay = butil::gettimeofday_us() - create_realtime_us();
        }
    } else {
        if (!_first_video_message_delay) {
            _first_video_message_delay = butil::gettimeofday_us() - create_realtime_us();
        }
    }
    const size_t old_size = _flv_data.size();
    butil::Status st = _flv_writer.Write(msg);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to write video message into FLV, " << st;
        return -1;
    }
    return flush_or_revert(old_size);
}

butil::EndPoint FlvDownloader::remote_side() const {
    return _flv_pa ? _flv_pa->remote_side() : butil::EndPoint();
}

butil::EndPoint FlvDownloader::local_side() const {
    return _flv_pa ? _flv_pa->local_side() : butil::EndPoint();
}

int FlvDownloader::flush_or_revert(ssize_t size_on_failure) {
    const size_t data_size = _flv_data.size();
    if (!data_size) {
        return 0;
    }
    print_event_log(EVENT_LOG_START_PLAY);
    if (_flv_pa != NULL && _flv_pa->Write(_flv_data) == 0) {
        _sent_bytes += data_size;
        _flv_data.clear();
        return 0;
    }
    if (size_on_failure >= 0) {
        if ((size_t)size_on_failure <= data_size) {
            _flv_data.pop_back(data_size - size_on_failure);
        } else {
            LOG(ERROR) << "Invalid size_on_failure=" << size_on_failure
                       << ", data_size=" << data_size;
        }
    }
    return -1;
}

HttpStreamingServiceOptions::HttpStreamingServiceOptions()
    : proxy_hls(false) {
}

void remove_from_service(RtmpForwardService* forward_service,
                         std::string key,
                         brpc::RtmpStreamBase* stream) {
    forward_service->remove_player(key, stream);
}

static butil::StringPiece get_vhost_from_query(const brpc::URI& uri) {
    // vhost to http-flv/HLS is passed by query string of the HTTP request.
    // Similarly with RTMP : vhost > wshost/wsHost > domain.
    butil::StringPiece vhost_raw;
    const std::string* vhost_pstr = uri.GetQuery("vhost");
    if (NULL == vhost_pstr) {
        vhost_pstr = uri.GetQuery("wshost");
        if (NULL == vhost_pstr) {
            vhost_pstr = uri.GetQuery("wsHost");
            if (NULL == vhost_pstr) {
                vhost_pstr = uri.GetQuery("domain");
            }
        }
    }
    if (vhost_pstr) {
        vhost_raw = *vhost_pstr;
        // vhost cannot have port.
        const size_t colon_pos = vhost_raw.find_last_of(':');
        if (colon_pos != butil::StringPiece::npos) {
            vhost_raw.remove_suffix(vhost_raw.size() - colon_pos);
        }
    }
    return vhost_raw;
}

static std::string get_vhost_from_uri(const brpc::URI& uri) {
    return get_vhost(uri.host(), get_vhost_from_query(uri));
}

HttpStreamingServiceImpl::HttpStreamingServiceImpl(
    RtmpForwardService* forward_service,
    const HttpStreamingServiceOptions& httpopt)
    : _forward_service(forward_service)
    , _httpopt(httpopt) {
    if (_httpopt.proxy_hls &&
        rtmp_options().proxy_to.find("://") != std::string::npos) {
        // HLS will be proxied via http calls to the naming service, to avoid
        // re-initializations of naming-service/tcp-connection/channel, we
        // create a channel to be shared with later http calls.
        std::unique_ptr<brpc::Channel> chan(new brpc::Channel);
        brpc::ChannelOptions opt;
        opt.protocol = brpc::PROTOCOL_HTTP;
        opt.connect_timeout_ms =
            (FLAGS_connect_timeout_ms > 0 ?
             FLAGS_connect_timeout_ms : FLAGS_timeout_ms / 3);
        opt.timeout_ms = FLAGS_timeout_ms;
        if (chan->Init(rtmp_options().proxy_to.c_str(),
                       rtmp_options().proxy_lb.c_str(), &opt) != 0) {
            LOG(ERROR) << "Fail to init channel to " << rtmp_options().proxy_to;
        } else {
            _proxy_http_channel.swap(chan);
        }
    }
}

static int get_app_stream_vhost_from_uri(const brpc::URI& uri,
                                         const butil::StringPiece& app_and_stream,
                                         butil::StringPiece *app,
                                         butil::StringPiece *stream_name,
                                         std::string* vhost) {
    bool host_in_url = false;
    int start_pos = 0;
    // The format of "r/host/app/stream.flv?vhost=xxxx" is redirected from 302 server
    if (app_and_stream.starts_with("r/")) {
        host_in_url = true;
        start_pos = 2;
    }

    if (host_in_url) {
        const size_t slash_pos_app = app_and_stream.find('/', start_pos);
        if (slash_pos_app == butil::StringPiece::npos) {
            LOG(ERROR) << "Invalid app_and_stream=" << app_and_stream;
            return -1;
        }
        const size_t slash_pos_stream = app_and_stream.find('/', slash_pos_app + 1);
        if (slash_pos_app == butil::StringPiece::npos) {
            LOG(ERROR) << "Invalid app_and_stream=" << app_and_stream;
            return -1;
        }
        *vhost = get_vhost_from_query(uri).as_string();
        if (vhost->empty()) {
            const size_t colon_pos = app_and_stream.find_first_of(':');
            if (colon_pos != butil::StringPiece::npos && colon_pos < slash_pos_app) {
                *vhost = app_and_stream.substr(start_pos, colon_pos - start_pos).as_string();
            } else {
                *vhost = app_and_stream.substr(start_pos, slash_pos_app - start_pos).as_string();
            }
        }
        *app = app_and_stream.substr(slash_pos_app + 1, 
                                     slash_pos_stream - slash_pos_app - 1);
        *stream_name = app_and_stream.substr(slash_pos_stream + 1,
                                             app_and_stream.size() - slash_pos_stream - 1);
    } else {
        *vhost = get_vhost_from_uri(uri);
        const size_t slash_pos = app_and_stream.find('/');
        if (slash_pos == std::string::npos) {
            return -1;
        }
        *app = app_and_stream.substr(0, slash_pos);
        *stream_name = app_and_stream.substr(slash_pos + 1,
                                  app_and_stream.size() - slash_pos - 1);
    }
    return 0;
}

void HttpStreamingServiceImpl::stream_flv(
    google::protobuf::RpcController* cntl_base,
    const HttpRequest*,
    HttpResponse*,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);
    brpc::URI& uri = cntl->http_request().uri();
    const std::string& app_and_stream = cntl->http_request().unresolved_path();
    butil::StringPiece app;
    butil::StringPiece stream_name;
    std::string vhost;
    if (get_app_stream_vhost_from_uri(uri, app_and_stream, &app,
                                      &stream_name, &vhost) < 0) {
        cntl->SetFailed(brpc::EREQUEST, "Invalid path=%s",
                            uri.path().c_str());
        return;
    }
    std::string original_vhost = vhost;
    std::string charge_key = make_key(vhost, app, stream_name);
    std::string play_key = charge_key;
    std::string queries = uri.query();
    if (rtmp_options().auth_play == "config") {
        // Do the authentication by user configuration.
        // It should be implemented by service provider.
    }
    
    VLOG(99) << "Converting " << play_key << " to http-flv";
    // Demonstrating 302 redirecting.
    // if (cntl->server()->listen_address().port == 8079) {
    //     cntl->http_response().SetHeader("Connection", "close");
    //     cntl->http_response().SetHeader("Cache-Control", "no-cache");
    //     cntl->http_response().SetHeader("Location", "http://brpc.baidu.com:8070/live/" + stream_name + ".flv");
    //     cntl->http_response().set_status_code(302);
    //     return;
    // }
    butil::intrusive_ptr<brpc::ProgressiveAttachment> flv_pa;
    bool audio_enabled = true;
    bool video_enabled = true;
    if (!queries.empty()) {
        bool audio_only = false;
        bool video_only = false;
        // Remove audio-only/video-only from queries so that pulled streams
        // are not affected
        remove_audio_video_selections(&queries, &audio_only, &video_only);
        if (audio_only && video_only) {
            LOG(WARNING) << MEDIA_SERVER_AUDIO_ONLY_TAG " and "
                MEDIA_SERVER_VIDEO_ONLY_TAG " are not allowed to be present "
                "at same time, ignore both";
        } else {
            video_enabled = !audio_only;
            audio_enabled = !video_only;
        }
    }

    bool keep_time = is_keep_time();
    butil::intrusive_ptr<brpc::RtmpStreamBase> flv_stream;
    cntl->http_response().set_content_type("video/x-flv");
    cntl->http_response().SetHeader("Access-Control-Allow-Origin", "*");
    flv_pa.reset(cntl->CreateProgressiveAttachment(brpc::FORCE_STOP));
    if (flv_pa == NULL) {
        cntl->SetFailed("The socket was just failed");
        return;
    }
    flv_stream.reset(new FlvDownloader(flv_pa.get(), _forward_service, play_key, charge_key,
                          audio_enabled, video_enabled));
    butil::Status st = _forward_service->add_player(play_key, queries, flv_stream.get(),
                                                    0, keep_time);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    flv_pa->NotifyOnStopped(
        brpc::NewCallback<RtmpForwardService*, std::string,
        brpc::RtmpStreamBase*>(remove_from_service, _forward_service,
                                     play_key, flv_stream.get()));
}

////////////////////  HLS  //////////////////////

static size_t get_ts_entry_count(void*) {
    TsEntryMap* m = butil::has_leaky_singleton<TsEntryMap>();
    return m != NULL ? m->size() : 0;
}
struct IsTsEntryIdle {
    IsTsEntryIdle() : _now_ms(butil::gettimeofday_ms()) {}
    bool operator()(const TsEntry& val) const {
        return val.is_idle_more_than(
            val.expiration_ms(), _now_ms);
    }
private:
    int64_t _now_ms;
};
static void* remove_idle_entry_thread(void*) {
    bvar::PassiveStatus<size_t> ts_entry_count(
        "hls_proxy_ts_count", get_ts_entry_count, NULL);
    std::vector<std::pair<TsEntryKey, butil::intrusive_ptr<TsEntry> > > removals;
    TsEntryMap* m = get_ts_entry_map();
    while (true) {
        IsTsEntryIdle filter;
        m->remove_by(filter, &removals);
        removals.clear();
        bthread_usleep(1000000/*1s*/);
    }
    return NULL;
}
struct CreateRemoveIdleEntryThread {
    bthread_t th;
    CreateRemoveIdleEntryThread() {
        CHECK_EQ(0, bthread_start_background(
                     &th, NULL, remove_idle_entry_thread, NULL));
    }
};
void get_or_new_ts_entry_from_cache(const std::string& key,
                                    int64_t seq_num,
                                    bool started_with_keyframe,
                                    butil::intrusive_ptr<TsEntry>* ptr,
                                    bool* did_new_out,
                                    int64_t expiration_ms) {
    const TsEntryKey entry_key{key, seq_num, started_with_keyframe, expiration_ms};
    bool did_new = get_ts_entry_map()->get_or_new(entry_key, ptr);
    if (did_new_out) {
        *did_new_out = did_new;
    }
    butil::get_leaky_singleton<CreateRemoveIdleEntryThread>();
}

ProxyHttp::ProxyHttp(HttpStreamingServiceImpl* service,
                     brpc::Controller* server_cntl,
                     google::protobuf::Closure* server_done,
                     const std::string& key,
                     bool progressive_body)
    : _service(service)
    , _server_cntl(server_cntl)
    , _server_done(server_done)
    , _key(key)
    , _progressive_body(progressive_body) {}

ProxyHttp::~ProxyHttp() {
    _server_done->Run();
}

void ProxyHttp::on_get_target_server(const butil::Status& status,
                                     const char* server_addr,
                                     const char* lb_algo) {
    std::unique_ptr<ProxyHttp> delete_self(this);
    if (!status.ok()) {
        _server_cntl->SetFailed(status.error_code(), "%s", status.error_cstr());
        return;
    }
    if (_server_cntl->request_protocol() != brpc::PROTOCOL_HTTP) {
        _server_cntl->SetFailed(EPERM, "The server request is not HTTP");
        return;
    }
    brpc::Channel new_chan;
    brpc::Channel* using_chan = NULL;
    if (_service->rtmp_options().proxy_to == server_addr &&
        _service->rtmp_options().proxy_lb == lb_algo) {
        using_chan = _service->_proxy_http_channel.get();
    } else {
        using_chan = &new_chan;
        brpc::ChannelOptions opt;
        opt.protocol = brpc::PROTOCOL_HTTP;
        opt.connect_timeout_ms =
            (FLAGS_connect_timeout_ms > 0 ?
             FLAGS_connect_timeout_ms : FLAGS_timeout_ms / 3);
        opt.timeout_ms = FLAGS_timeout_ms;
        if (new_chan.Init(server_addr, lb_algo, &opt) != 0) {
            _server_cntl->SetFailed(-1, "Fail to init channel to %s",
                                    server_addr);
            return;
        }
    }
    _client_cntl.http_request() = _server_cntl->http_request();
    // Remove "Host" which is not the server to be proxied.
    _client_cntl.http_request().RemoveHeader("host");
    _client_cntl.http_request().uri().set_host("");

    // Always set request_code to make consistent hashing work.
    if (!_key.empty()) {
        _client_cntl.set_request_code(
            brpc::policy::MurmurHash32(_key.data(), _key.size()));
    }

    // Keep content as it is.
    _client_cntl.request_attachment() = _server_cntl->request_attachment();
    if (_progressive_body) {
        _client_cntl.response_will_be_read_progressively();
    }
    if (_controller_modifier) {
        _controller_modifier->modify(&_client_cntl);
    }
    using_chan->CallMethod(NULL, &_client_cntl, NULL, NULL, delete_self.release());
}

class CopyToWriterProgressively : public brpc::ProgressiveReader {
public:
    CopyToWriterProgressively(brpc::ProgressiveAttachment* pa)
        : _pa(pa) {}
    // @ProgressiveReader
    butil::Status OnReadOnePart(const void* data, size_t length);
    void OnEndOfMessage(const butil::Status&);
private:
    butil::intrusive_ptr<brpc::ProgressiveAttachment> _pa;
};

butil::Status CopyToWriterProgressively::OnReadOnePart(
    const void* data, size_t length) {
    do {
        if (_pa->Write(data, length) == 0) {
            return butil::Status::OK();
        }
        if (errno != brpc::EOVERCROWDED) {
            return butil::Status(errno, "%s", berror(errno));
        }
        // Wait for a while.
        bthread_usleep(10000);
    } while (true);
}

void CopyToWriterProgressively::OnEndOfMessage(const butil::Status& st) {
    delete this;
}

class CopyToTsEntryProgressively : public brpc::ProgressiveReader {
public:
    CopyToTsEntryProgressively(butil::intrusive_ptr<TsEntry> & ts_entry) {
        _ts_entry.swap(ts_entry);
    }
    // @ProgressiveReader
    butil::Status OnReadOnePart(const void* data, size_t length);
    void OnEndOfMessage(const butil::Status&);
private:
    butil::intrusive_ptr<TsEntry> _ts_entry;
};
butil::Status CopyToTsEntryProgressively::OnReadOnePart(
    const void* data, size_t length) {
    do {
        butil::IOBuf buf;
        buf.append(data, length);
        if (_ts_entry->append_ts_data(buf, 0) == 0) {
            return butil::Status::OK();
        }
        return butil::Status(-1, "Fail to append_ts_data");
    } while (true);
}

void CopyToTsEntryProgressively::OnEndOfMessage(const butil::Status& st) {
    _ts_entry->end_writing(0);
    delete this;
}

void ProxyHttp::Run() {
    std::unique_ptr<ProxyHttp> delete_self(this);
    
    _server_cntl->http_response() = _client_cntl.http_response();
    // Notice that we don't set RPC to failed on http errors because we
    // want to pass unchanged content to the users otherwise RPC replaces
    // the content with ErrorText.
    if (_client_cntl.Failed()) {
        if (_failed_response_handler) {
            _failed_response_handler->Handle(_client_cntl, _server_cntl);
        } else {
            if (_client_cntl.ErrorCode() != brpc::EHTTP) {
                _server_cntl->SetFailed(_client_cntl.ErrorCode(),
                                        "%s", _client_cntl.ErrorText().c_str());
            }
        }
        return;
    } else if (!_client_cntl.is_response_read_progressively()) {
        if (_response_attachment_handler) {
            _response_attachment_handler->Handle(_client_cntl, _server_cntl);
        } else {
            // Copy content.
            _server_cntl->response_attachment() = _client_cntl.response_attachment();
        }
    } else if (_ts_entry == NULL) {
        brpc::ProgressiveAttachment* pa =
            _server_cntl->CreateProgressiveAttachment(brpc::FORCE_STOP);
        if (pa == NULL) {
            _server_cntl->SetFailed(brpc::EFAILEDSOCKET,
                                    "Fail to create progressive attachment");
            return;
        }
        _client_cntl.ReadProgressiveAttachmentBy(
            new CopyToWriterProgressively(pa));
    } else {
        _client_cntl.ReadProgressiveAttachmentBy(
            new CopyToTsEntryProgressively(_ts_entry));
    }
}

bool is_start_with_keyframe(const std::string* user_agent) {
    if (user_agent && butil::StringPiece(*user_agent).starts_with("Lavf")) {
        return true;
    }
    return false;
}

void get_ts_segment_policy(const std::string& key, int64_t* ts_duration_ms,
                           int64_t* ts_num_per_m3u8) {
    // Implement this function if ts_duration_ms and ts_num_per_m3u8 needs
    // to be customed.
}

void HttpStreamingServiceImpl::get_master_playlist(
    google::protobuf::RpcController* cntl_base,
    const HttpRequest*,
    HttpResponse*,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);
    brpc::URI& uri = cntl->http_request().uri();
    const std::string& app_and_stream = cntl->http_request().unresolved_path();
    butil::StringPiece app;
    butil::StringPiece stream_name;
    std::string original_vhost;
    if (get_app_stream_vhost_from_uri(uri, app_and_stream, &app,
                                      &stream_name, &original_vhost) < 0) {
        cntl->SetFailed(brpc::EREQUEST, "Invalid path=%s",
                            uri.path().c_str());
        return;
    }
    std::string vhost = original_vhost;
    std::string play_key = make_key(vhost, app, stream_name);
    std::string queries = uri.query();
    if (rtmp_options().auth_play == "config") {
        // Do the authentication by user configuration.
        // It should be implemented by service provider.
    }
    const int port = rtmp_options().port;
    if (http_options().proxy_hls &&
        !rtmp_options().proxy_to.empty() &&
        !is_publish_proxy(stream_name)) {
        std::string host_str;
        if (FLAGS_use_host_in_m3u8) {
            host_str = butil::string_printf("%s:%d", uri.host().c_str(), uri.port());
        } else {
            host_str = butil::string_printf("%s:%d", butil::my_ip_cstr(), port);
        }
        uri.SetQuery("specified_host", host_str);
        if (is_user_defined_vhost(vhost)) {
            uri.SetQuery("vhost", vhost);
        }
        return _forward_service->get_target_server(
            vhost, app, stream_name, uri.query(), false,
            new ProxyHttp(this, cntl, done_guard.release(), play_key, false));
    }

    const std::string* specified_host = uri.GetQuery("specified_host");
    butil::IOBufBuilder os;
    os << "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=2560000\nhttp://";
    if (specified_host != NULL && !specified_host->empty()) {
        // The request is from another media-server which proxies HLS requests
        // (generally inside a CDN node using consistent-hashing LB)
        os << *specified_host;
    } else {
        os << butil::my_ip() << ':' << port;
    }
    // Remove specified_host from queries which will be after media playlists
    // and visible to users.
    uri.RemoveQuery("specified_host");
    specified_host = NULL;
    const HLSUserId user_id = generate_hls_user_id();
    os << '/' << vhost << '/' << app << '/' << stream_name
       << ".lss" << user_id << ".dynamic.m3u8";
    const std::string* vhost_pstr = uri.GetQuery("vhost");
    int firstq = 0;
    if (vhost_pstr == NULL) {
        os << (!firstq++ ? '?': '&') << "vhost=" << original_vhost; 
    }
    if (!uri.query().empty()) {
        // media playlist inherits queries to master playlist
        os << (!firstq++ ? '?': '&') << uri.query();
    }
    os << '\n';
    cntl->http_response().set_content_type("application/vnd.apple.mpegurl");
    cntl->http_response().SetHeader("Access-Control-Allow-Origin", "*");
    os.move_to(cntl->response_attachment());
    // Touch the entry for the user_id so that get_media_playlist() can get
    // the user_id successfully. We don't add user entry in get_media_playlist()
    // because after restarting accesses to previous media playlists should be
    // invalid and rejected.
    int64_t ts_duration_ms = FLAGS_ts_duration_ms;
    int64_t ts_num_per_m3u8 = FLAGS_ts_max_stored;
    get_ts_segment_policy(play_key, &ts_duration_ms, &ts_num_per_m3u8);
    butil::intrusive_ptr<HLSUserMeta> dummy_user_meta;
    get_or_new_hls_user_meta(user_id, ts_duration_ms * get_stored_ts_num(ts_num_per_m3u8), &dummy_user_meta);
    bool keep_time = is_keep_time();
    // In order to support ffplay
    bool started_with_kf_in_ts =
        is_start_with_keyframe(cntl->http_request().GetHeader("User-Agent"));
    // Make sure TsQueue on the key is working.
    butil::intrusive_ptr<TsQueue> tsq;
    const bool did_new = get_or_new_ts_queue({play_key, started_with_kf_in_ts, ts_duration_ms, 
                                              ts_num_per_m3u8},
                                             &tsq);
    if (did_new) {
        butil::Status st = _forward_service->add_player(play_key, queries, tsq.get(), 0, keep_time);
        if (!st.ok()) {
            tsq->SendStopMessage(st.error_cstr());
            cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
            return;
        }
        tsq->on_idle(
            brpc::NewCallback<RtmpForwardService*, std::string,
            brpc::RtmpStreamBase*>(remove_from_service, _forward_service,
                                         play_key, tsq.get()));
    }
}

void HttpStreamingServiceImpl::get_media_playlist(
    google::protobuf::RpcController* cntl_base,
    const HttpRequest*,
    HttpResponse*,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);
    const brpc::URI& uri = cntl->http_request().uri();
    const std::string& key_and_lssid = cntl->http_request().unresolved_path();
    // Find the last dot. key containing dots is OK.
    const size_t dot_pos = key_and_lssid.find_last_of('.');
    if (dot_pos == std::string::npos) {
        cntl->SetFailed(brpc::EREQUEST, "Invalid M3U8 path=%s",
                        uri.path().c_str());
        return;
    }
    const std::string key = key_and_lssid.substr(0, dot_pos);
    const size_t vhost_pos = key.find_first_of('/');
    if (vhost_pos == std::string::npos) {
        cntl->SetFailed(brpc::EREQUEST, "Invalid M3U8 path=%s",
                        uri.path().c_str());
        return;
    }
    std::string vhost = key.substr(0, vhost_pos);
    const std::string* vhost_pstr = uri.GetQuery("vhost");
    if (vhost_pstr != NULL) {
        vhost = *vhost_pstr;
    }

    if (http_options().proxy_hls &&
        !rtmp_options().proxy_to.empty() &&
        !is_publish_proxy(key)) {
        return _forward_service->get_target_server(key, false,
            new ProxyHttp(this, cntl, done_guard.release(), key, false));
    }

    butil::StringPiece lssid_str(key_and_lssid.data() + dot_pos + 1,
                                key_and_lssid.size() - dot_pos - 1);
    if (!lssid_str.starts_with("lss")) {
        cntl->SetFailed(brpc::EREQUEST, "Invalid M3U8 path=%s",
                        uri.path().c_str());
        return;        
    }
    int64_t ts_duration_ms = FLAGS_ts_duration_ms;
    int64_t ts_num_per_m3u8 = FLAGS_ts_max_stored;
    get_ts_segment_policy(key, &ts_duration_ms, &ts_num_per_m3u8);
    const uint64_t user_id = strtoull(lssid_str.data() + 3, NULL, 10);    
    butil::intrusive_ptr<HLSUserMeta> user_meta;
    if (!get_hls_user_meta(user_id, ts_duration_ms * get_stored_ts_num(ts_num_per_m3u8), &user_meta)) {
        cntl->SetFailed(brpc::EREQUEST, "Invalid user_id=%" PRIu64,
                        user_id);
        return;
    }
    int64_t new_start_seq_num = user_meta->start_seq_num;
    
    const size_t slash_pos = key.find_last_of('/');
    if (slash_pos == std::string::npos) {
        cntl->SetFailed(brpc::EREQUEST, "Invalid M3U8 path=%s",
                        uri.path().c_str());
        return;
    }
    butil::StringPiece stream_name = butil::StringPiece(key).substr(slash_pos + 1);

    // In order to support ffplay
    bool started_with_kf_in_ts =
        is_start_with_keyframe(cntl->http_request().GetHeader("User-Agent"));
    butil::intrusive_ptr<TsQueue> tsq;
    if (!get_ts_queue({key, started_with_kf_in_ts, ts_duration_ms, ts_num_per_m3u8}, &tsq)) {
        cntl->SetFailed(brpc::ENOMETHOD,
                        "Fail to find TsQueue for key=%s"
                        ", started_with_kf_in_ts=%d", 
                        key.c_str(), started_with_kf_in_ts);
        return;
    }
    char queries[128];
    snprintf(queries, sizeof(queries), "lssid=%" PRIu64 "&vhost=%s", user_id, vhost.c_str());
    butil::IOBufBuilder os;
    int nretries = 0;
    while (nretries < FLAGS_get_media_playlist_times && !bthread_stopped(bthread_self())) {
        if (tsq->generate_media_playlist(
                os, &new_start_seq_num, stream_name, queries) == 0) {
            break;
        }
        nretries++;
        bthread_usleep(FLAGS_retry_interval_ms * 1000L);
    }
    if (nretries == FLAGS_get_media_playlist_times) {
        cntl->SetFailed(errno, "Fail to generate media_playlist for key=%s",
                        key.c_str());
        return;
    }
    if (user_meta->start_seq_num != new_start_seq_num) {
        user_meta->start_seq_num = new_start_seq_num;
    }
    cntl->http_response().set_content_type("application/vnd.apple.mpegurl");
    cntl->http_response().SetHeader("Access-Control-Allow-Origin", "*");
    os.move_to(cntl->response_attachment());
}

void HttpStreamingServiceImpl::stream_ts(
    google::protobuf::RpcController* cntl_base,
    const HttpRequest*,
    HttpResponse*,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    const brpc::URI& uri = cntl->http_request().uri();
    const std::string& key_and_sn = cntl->http_request().unresolved_path();
    // Find the last dot. key containing dots is OK.
    const size_t dot_pos = key_and_sn.find_last_of('.');
    if (dot_pos == std::string::npos) {
        cntl->SetFailed(brpc::EREQUEST, "Invalid TS path=%s",
                        uri.path().c_str());
        return;
    }
    const std::string key = key_and_sn.substr(0, dot_pos);
    butil::StringPiece sn_str(key_and_sn.data() + dot_pos + 1,
                             key_and_sn.size() - dot_pos - 1);
    if (!sn_str.starts_with("seq")) {
        cntl->SetFailed(brpc::EREQUEST, "Invalid TS path=%s",
                        uri.path().c_str());
        return;        
    }
    const uint64_t seq_num = strtoull(sn_str.data() + 3, NULL, 10);
    const size_t vhost_pos = key.find_first_of('/');
    if (vhost_pos == std::string::npos) {
        cntl->SetFailed(brpc::EREQUEST, "Invalid TS path=%s",
                        uri.path().c_str());
        return;        
    }
    std::string vhost = key.substr(0, vhost_pos);
    const std::string* vhost_pstr = uri.GetQuery("vhost");
    if (vhost_pstr != NULL) {
        vhost = *vhost_pstr;
    }
    std::string charge_key = key;
    charge_key.replace(0, vhost_pos, vhost);
    // In order to support ffplay
    bool started_with_kf_in_ts =
        is_start_with_keyframe(cntl->http_request().GetHeader("User-Agent"));
    int64_t ts_duration_ms = FLAGS_ts_duration_ms;
    int64_t ts_num_per_m3u8 = FLAGS_ts_max_stored;
    get_ts_segment_policy(key, &ts_duration_ms, &ts_num_per_m3u8);
    if (http_options().proxy_hls &&
        !rtmp_options().proxy_to.empty() &&
        !is_publish_proxy(key)) {
        brpc::ProgressiveAttachment* pa =
            cntl->CreateProgressiveAttachment(brpc::FORCE_STOP);
        if (pa == NULL) {
            cntl->SetFailed(brpc::EFAILEDSOCKET,
                            "Fail to create progressive attachment");
            return;
        }
        butil::intrusive_ptr<TsEntry> ts_entry;
        bool did_new = false;
        get_or_new_ts_entry_from_cache(key, seq_num, started_with_kf_in_ts,
                                       &ts_entry, &did_new, ts_duration_ms * get_stored_ts_num(ts_num_per_m3u8));
        ts_entry->add_downloader(pa, rtmp_options().internal_service, charge_key);
        if (!did_new) {
            cntl->http_response().set_content_type("video/MP2T");
            cntl->http_response().SetHeader("Access-Control-Allow-Origin", "*");
            return;
        }
        if (ts_entry->begin_writing(0, started_with_kf_in_ts) != 0) {
            cntl->SetFailed("Fail to begin_writing");
            return;
        }
        ProxyHttp* done = new ProxyHttp(this, cntl, done_guard.release(), key, true);
        done->ts_entry().swap(ts_entry);
        return _forward_service->get_target_server(key, false, done);
    }
    
    cntl->http_response().set_content_type("video/MP2T");
    cntl->http_response().SetHeader("Access-Control-Allow-Origin", "*");
    butil::intrusive_ptr<brpc::ProgressiveAttachment> ts_pa(
        cntl->CreateProgressiveAttachment(brpc::FORCE_STOP));
    if (ts_pa == NULL) {
        cntl->SetFailed("The socket was just failed");
        return;
    }
    butil::intrusive_ptr<TsQueue> tsq;
    if (!get_ts_queue({key, started_with_kf_in_ts, ts_duration_ms, ts_num_per_m3u8}, &tsq)) {
        cntl->SetFailed(brpc::ENOMETHOD,
                        "Fail to find TsQueue for key=%s"
                        ", started_with_kf_in_ts=%d", 
                        key.c_str(), started_with_kf_in_ts);
        return;
    }
    butil::intrusive_ptr<TsEntry> ts_entry;
    if (!tsq->get_or_new_ts_entry(seq_num, &ts_entry)) {
        cntl->SetFailed(brpc::ENOMETHOD, "Fail to get TS@%" PRIu64, seq_num);
        return;
    }
    VLOG(99) << "Converting " << key << " to " << *ts_entry;
    ts_entry->add_downloader(ts_pa.get(), rtmp_options().internal_service, charge_key);
}

void HttpStreamingServiceImpl::get_crossdomain_xml(
    google::protobuf::RpcController* cntl_base,
    const HttpRequest*,
    HttpResponse*,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);
    const std::string vhost = get_vhost_from_uri(cntl->http_request().uri());
    cntl->response_attachment().append("<cross-domain-policy>"
                                       "<allow-access-from domain=\"*\"/>"
                                       "</cross-domain-policy>");
    cntl->http_response().set_content_type("text/xml");
}

void HttpStreamingServiceImpl::play_hls(
    google::protobuf::RpcController* cntl_base,
    const HttpRequest*,
    HttpResponse*,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);
    cntl->http_response().set_content_type("text/html");
    const std::string* m3u8url = cntl->http_request().uri().GetQuery("fileUrl");
    if (m3u8url == NULL) {
        cntl->SetFailed(brpc::EREQUEST, "query `fileUrl' must be specified");
        return;
    }
    butil::IOBufBuilder os;
    os << "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n"
        "  <meta charset=\"utf-8\">\n"
        "  <script>\n"
        "  !function(a){var b=/iPhone/i,c=/iPod/i,d=/iPad/i,e=/(?=.*\bAndroid\b)(?=.*\bMobile\b)/i,f=/Android/i,g=/(?=.*\bAndroid\b)(?=.*\bSD4930UR\b)/i,h=/(?=.*\bAndroid\b)(?=.*\b(?:KFOT|KFTT|KFJWI|KFJWA|KFSOWI|KFTHWI|KFTHWA|KFAPWI|KFAPWA|KFARWI|KFASWI|KFSAWI|KFSAWA)\b)/i,i=/IEMobile/i,j=/(?=.*\bWindows\b)(?=.*\bARM\b)/i,k=/BlackBerry/i,l=/BB10/i,m=/Opera Mini/i,n=/(CriOS|Chrome)(?=.*\bMobile\b)/i,o=/(?=.*\bFirefox\b)(?=.*\bMobile\b)/i,p=new RegExp('(?:Nexus 7|BNTV250|Kindle Fire|Silk|GT-P1000)','i'),q=function(a,b){return a.test(b)},r=function(a){var r=a||navigator.userAgent,s=r.split('[FBAN');return'undefined'!=typeof s[1]&&(r=s[0]),s=r.split('Twitter'),'undefined'!=typeof s[1]&&(r=s[0]),this.apple={phone:q(b,r),ipod:q(c,r),tablet:!q(b,r)&&q(d,r),device:q(b,r)||q(c,r)||q(d,r)},this.amazon={phone:q(g,r),tablet:!q(g,r)&&q(h,r),device:q(g,r)||q(h,r)},this.android={phone:q(g,r)||q(e,r),tablet:!q(g,r)&&!q(e,r)&&(q(h,r)||q(f,r)),device:q(g,r)||q(h,r)||q(e,r)||q(f,r)},this.windows={phone:q(i,r),tablet:q(j,r),device:q(i,r)||q(j,r)},this.other={blackberry:q(k,r),blackberry10:q(l,r),opera:q(m,r),firefox:q(o,r),chrome:q(n,r),device:q(k,r)||q(l,r)||q(m,r)||q(o,r)||q(n,r)},this.seven_inch=q(p,r),this.any=this.apple.device||this.android.device||this.windows.device||this.other.device||this.seven_inch,this.phone=this.apple.phone||this.android.phone||this.windows.phone,this.tablet=this.apple.tablet||this.android.tablet||this.windows.tablet,'undefined'==typeof window?this:void 0},s=function(){var a=new r;return a.Class=r,a};'undefined'!=typeof module&&module.exports&&'undefined'==typeof window?module.exports=r:'undefined'!=typeof module&&module.exports&&'undefined'!=typeof window?module.exports=s():'function'==typeof define&&define.amd?define('isMobile',[],a.isMobile=s()):a.isMobile=s()}(this);\n"
        "  (function () {\n"
        "    if (isMobile.apple.phone) {\n"
        "      document.location = decodeURIComponent('" << *m3u8url << "');\n"
        "    } else if (isMobile.android.phone || isMobile.seven_inch) {\n"
        "      document.location = 'http://cyberplayer.bcelive.com/live/index.html?fileUrl=" << *m3u8url << "';\n"
        "    }\n"
        "  })();\n"
        "  </script>\n"
        "  <script src=\"/get_hls_min\"></script>\n"
        "  <style type=\"text/css\">\n"
        "  body {\n"
        "    background-color: black;\n"
        "  }\n"
        "  .zoomed_mode{\n"
        "    position: absolute;\n"
        "    top: 0px;\n"
        "    right: 0px;\n"
        "    bottom: 0px;\n"
        "    left: 0px;\n"
        "    margin: auto;\n"
        "    max-height: 100%;\n"
        "    width: 100%;\n"
        "  }</style>\n"
        "</head>\n"
        "<body>\n"
        "<video id='video' class='zoomed_mode' controls></video>\n"
        "<script>\n"
        "  if(Hls.isSupported()) {\n"
        "    var video = document.getElementById('video');\n"
        "    var hls = new Hls();\n"
        "    var m3u8Url = decodeURIComponent('" << *m3u8url << "');\n"
        "    hls.loadSource(m3u8Url);\n"
        "    hls.attachMedia(video);\n"
        "    hls.on(Hls.Events.MANIFEST_PARSED,function() {\n"
        "      video.play();\n"
        "    });\n"
        "  }\n"
        "</script>\n"
        "</body></html>\n";
    os.move_to(cntl->response_attachment());
    cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
}

static const char* g_last_modified = "Wed, 16 Sep 2015 01:25:30 GMT";

static void SetExpires(brpc::HttpHeader* header, time_t seconds) {
    char buf[256];
    time_t now = time(0);
    brpc::Time2GMT(now, buf, sizeof(buf));
    header->SetHeader("Date", buf);
    brpc::Time2GMT(now + seconds, buf, sizeof(buf));
    header->SetHeader("Expires", buf);
}

void HttpStreamingServiceImpl::get_hls_min(
    ::google::protobuf::RpcController* controller,
    const HttpRequest* /*request*/,
    HttpResponse* /*response*/,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = (brpc::Controller*)controller;
    cntl->http_response().set_content_type("application/javascript");
    SetExpires(&cntl->http_response(), 80000);

    const std::string* ims =
        cntl->http_request().GetHeader("If-Modified-Since");
    if (ims != NULL && *ims == g_last_modified) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_MODIFIED);
        return;
    }
    cntl->http_response().SetHeader("Last-Modified", g_last_modified);
    
    if (SupportGzip(cntl)) {
        cntl->http_response().SetHeader("Content-Encoding", "gzip");
        cntl->response_attachment().append(hls_min_js_iobuf_gzip());
    } else {
        cntl->response_attachment().append(hls_min_js_iobuf());
    }
}

void HttpStreamingServiceImpl::get_cdn_probe(
    ::google::protobuf::RpcController* controller,
    const HttpRequest* /*request*/,
    HttpResponse* /*response*/,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = (brpc::Controller*)controller;
    cntl->response_attachment().append("Service is fine");
}
