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

#ifndef MEDIA_SERVER_RTMP_FORWARD_SERVICE_H
#define MEDIA_SERVER_RTMP_FORWARD_SERVICE_H

#include "brpc/rtmp.h"
#include "brpc/builtin/tabbed.h"
#include "media_server.pb.h"
#include "shared_map.h"
#include "frame_queue.h"
#include "util.h"

class MonitoringServiceImpl;
class PlayerGroup;
class RtmpForwarder;
class ForwardProxy;
class RtmpForwardService;
struct PublishContext;

typedef butil::intrusive_ptr<PlayerGroup> PlayerGroupSharedPtr;

namespace brpc {
class Channel;
}

// Defined in media_server.cpp
bool at_cdn();

const char* const REQUESTID = "requestid";

// For bypassing authentication when auth_play is on.
// Should NOT be exposed to public.
#define MEDIA_SERVER_AUTH_MAGIC                                         \
    "21989829390109829389289389189839572727829734635269"                \
    "83217489371298473198473891748931748937128947318947"

struct RtmpForwardServiceOptions {
    // With default options.
    RtmpForwardServiceOptions();

    // [ REQUIRED ]
    // The port that the server containing this service will listen to.
    // Heartbeat sent by relay-inner or HLS playlist may contain this port (so
    // that receiver of the message may access via this port later).
    // Default: -1
    int port;

    // Only provide builtin services at this port, which is only accessible
    // from internal network
    // Default: -1
    int internal_port;

    // If the value is non-empty, this service proxies both publishing and
    // playing streams to the server (or servers).
    // If the value is empty, this service acts as a source site aggregating
    // publishing and playing streams together.
    // Default: ""
    std::string proxy_to;

    // Load balancing algorithm for proxying, only effective when proxy_to is
    // non-empty.
    // Default: ""
    std::string proxy_lb;

    // Multiple play streams over one connection
    // Default: true
    bool share_play_connection;

    // Multiple publish streams over one connection
    // Default: true
    bool share_publish_connection;

    // If this service is for internal usage. Internal traffic are not charged.
    // Default: false
    bool internal_service;

    // The method for authenticating play requests.
    // Default: ""
    std::string auth_play;

    // The method for authenticating publish requests.
    // Default: ""
    std::string auth_publish;
};

// The callback to run after getting the server for playing/publishing streams.
class OnGetTargetServer {
public:
    virtual ~OnGetTargetServer() {}

    // If status.ok() is true, parameters are valid.
    // otherwise, status.error_cstr() is why the process was failed.
    // `publish_ctx' may be non-NULL when publish_ctx to get_target_server()
    // is not NULL.
    virtual void on_get_target_server(const butil::Status& status,
                                      const char* server_addr,
                                      const char* lb_algo) = 0;

    void fail_to_get_target_server(const butil::Status& status)
    { on_get_target_server(status, NULL, NULL); }
};

enum PullWay {
    // The difference between PULL_BY_RTMP_INSIDE and PULL_BY_RTMP_OUTSIDE
    // is whether to turn on simplified rtmp.
    PULL_BY_RTMP_INSIDE,
    PULL_BY_RTMP_OUTSIDE,
};

class RtmpForwardService : public brpc::RtmpService {
public:
    RtmpForwardService();
    ~RtmpForwardService();

    // Must be called before usage.
    int init(const RtmpForwardServiceOptions& options);
    
    // @RtmpService
    brpc::RtmpServerStream*
    NewStream(const brpc::RtmpConnectRequest&);

    // Player management
    butil::Status add_player(const std::string& key,
                            const butil::StringPiece& queries,
                            brpc::RtmpStreamBase* player,
                            int64_t play_delay,
                            bool keep_original_time);
    void remove_player(const std::string& key,
                       const brpc::RtmpStreamBase* stream);
    size_t count_players(const std::string& key);
    
    // Publisher management
    int add_publisher(const std::string& key,
                      RtmpForwarder* publisher,
                      PlayerGroupSharedPtr* players_out,
                      int64_t publish_delay);
    int reset_publisher(const std::string& key, const RtmpForwarder* publisher);
    void remove_publisher(const std::string& key, const RtmpForwarder* publisher);

    // Get the server for pulling streams asynchronously.
    // vhost/app/stream_name are parameters for deciding the server.
    void get_target_server(const butil::StringPiece& vhost,
                           const butil::StringPiece& app,
                           const butil::StringPiece& stream_name,
                           const butil::StringPiece& queries,
                           bool is_publish,
                           OnGetTargetServer* done);
    // A variant. key=vhost+app+stream_name.
    void get_target_server(const butil::StringPiece& key,
                           bool is_publish,
                           OnGetTargetServer* done);

    // Get the client according to parameters and call done->Run(client) when
    // client is ready. If the client can't be created, done->Run(NULL) will
    // be called.
    void get_proxy_client(const butil::StringPiece& vhost,
                          const butil::StringPiece& app,
                          const butil::StringPiece& stream_name,
                          const butil::StringPiece& queries,
                          bool is_publish,
                          bool simplified_rtmp,
                          OnGetRtmpClient* done);

    // The options to init().
    const RtmpForwardServiceOptions& options() const { return _options; }

    int64_t get_player_count(const std::string& key);

    int set_user_config_in_frame_queue(const std::string& key,
                                       FrameQueueUserConfig* config);

private:
friend class MonitoringServiceImpl;
friend class PublishProxy;
    void get_server_to_publish_at_cdn(const butil::StringPiece& vhost,
                                      const butil::StringPiece& app,
                                      const butil::StringPiece& stream_name,
                                      OnGetTargetServer* done);

    // Get all PlayerGroups(and its key) inside.
    void list_all(std::vector<std::pair<std::string, PlayerGroupSharedPtr> >*);

    // Put the PlayerGroup associated with `key' to `*players'.
    void find_player_group(const std::string& key,
                           PlayerGroupSharedPtr* players);

    // Put the PlayerGroup associated with `key' to `*players'. If the key does
    // not exist, create a new one.
    void find_or_new_player_group(const std::string& key,
                                  PlayerGroupSharedPtr* players,
                                  int64_t launcher_create_time,
                                  int64_t first_play_or_publish_delay);

    static void* run_update_stats(void*);
    void update_stats();

    static void* run_print_stats(void*);    
    void print_stats();
    
    static void* run_dump_stats(void*);
    void dump_stats();

    static void* run_remove_idle_group(void*);
    void* remove_idle_group();

private:
    // temporarily store idle PlayerGroup which has no player at that time
    SharedMap<std::string, PlayerGroup> _idle_group_map;

    pthread_mutex_t _forward_map_mutex;
    typedef std::map<std::string, PlayerGroupSharedPtr> ForwardMap;
    ForwardMap _forward_map;
    RtmpForwardServiceOptions _options;

    bool _has_update_stats_thread;
    bool _has_dump_stats_thread;
    bool _has_print_stats_thread;
    bool _has_remove_idle_group_thread;
    bthread_t _update_stats_thread;
    bthread_t _dump_stats_thread;
    bthread_t _print_stats_thread;
    bthread_t _remove_idle_group_thread;
};

// For the "Media Server" tab in builtin services.
class MonitoringServiceImpl : public MonitoringService,
                              public brpc::Tabbed {
public:
    MonitoringServiceImpl(RtmpForwardService* forward_service)
        : _forward_service(forward_service) {}

    void monitor(google::protobuf::RpcController* cntl_base,
                 const HttpRequest*,
                 HttpResponse*,
                 google::protobuf::Closure* done);
    void players(google::protobuf::RpcController* cntl_base,
                 const HttpRequest*,
                 HttpResponse*,
                 google::protobuf::Closure* done);
    void urls(google::protobuf::RpcController* cntl_base,
              const HttpRequest*,
              HttpResponse*,
              google::protobuf::Closure* done);
    void GetTabInfo(brpc::TabInfoList* info_list) const;

    const RtmpForwardServiceOptions& rtmp_options() const
    { return _forward_service->options(); }

private:
    RtmpForwardService* _forward_service;
};

// published streams being proxied to another server are appended with this
// suffix to be distinguished from the same-named pulled streams.
static const char* const PUBLISH_PROXY_SUFFIX = ".pubproxy";

inline bool is_publish_proxy(const butil::StringPiece& name) {
    return name.ends_with(PUBLISH_PROXY_SUFFIX);
}

inline bool is_proxy_to_config(const butil::StringPiece& proxy_to)
{ return proxy_to == "config"; }

// Get a normalized vhost according to host&vhost given by user.
std::string get_vhost(const butil::StringPiece& host,
                      const butil::StringPiece& vhost);

// Returns an unique key for identifying a stream according to the parameters.
std::string make_key(const butil::StringPiece& vhost,
                     const butil::StringPiece& app,
                     const butil::StringPiece& stream_name);

#define MEDIA_SERVER_AUDIO_ONLY_TAG "only-audio"
#define MEDIA_SERVER_VIDEO_ONLY_TAG "only-video"

// Remove MEDIA_SERVER_AUDIO_ONLY_TAG and MEDIA_SERVER_VIDEO_ONLY_TAG from
// `queries' and set `audio_only' and `video_ony' to true respectively.
// Returns the modified queries.
void remove_audio_video_selections(std::string* queries,
                                   bool* audio_only,
                                   bool* video_only);
// Remove audio/video releated fields from the metadata to make players work
// correctly with audio-only/video-only streams.
void remove_audio_releated_fields(brpc::AMFObject* metadata);
void remove_video_releated_fields(brpc::AMFObject* metadata);

bool is_keep_time();

bool is_user_defined_vhost(const butil::StringPiece& vhost);

inline void to_lowercase(std::string* str) {
    std::transform(str->begin(), str->end(), str->begin(), ::tolower);
}

class QueryModifier {
public:
    virtual void modify(std::string* query) = 0;
    virtual ~QueryModifier() {}
};

#endif // MEDIA_SERVER_RTMP_FORWARD_SERVICE_H

