# media-server

media-server is a live streaming server based on [brpc](https://github.com/brpc/brpc) used in [Live Streaming Service](https://cloud.baidu.com/product/lss.html) of Baidu Cloud.

## Main features

* Support [origin server](docs/cn/origin_server.md) which streams can be pushed to and played from
* Support [edge server](docs/cn/edge_server.md) to proxy push/pull requests
* Support [rtmp](https://www.adobe.com/devnet/rtmp.html)/[flv](https://en.wikipedia.org/wiki/Flash_Video)/[hls](https://en.wikipedia.org/wiki/HTTP_Live_Streaming) play
* Support rtmp push
* Streams are uniquely determined by [vhost/app/stream_name](docs/cn/vhost_app_stream.md)
* Configurable push/pull [retry policy](docs/cn/retry_policy.md)
* Support [simplified rtmp protocol](docs/cn/simplified_rtmp.md) that eliminates rtmp handshake process
* Support [visual interface](docs/cn/http_service.md)(via http) to check the status of the current server/streams
* Support [low latency hls](docs/cn/low_latency_hls.md)(about one second slower than rtmp/flv)
* Support [video/audio only](docs/cn/av_only.md) live streaming
* Configurable [frame queue buffer](docs/cn/frame_queue.md) length(typically several seconds)
* Support [keep pulling](docs/cn/keep_pulling.md) streams for several seconds when no players are watching
* Support dumping [stream status](docs/cn/stream_status.md) into log for monitoring purpose
* Support different [re-publish policy](docs/cn/republish_policy.md)
* Support https
* All features brought by [brpc](https://github.com/brpc/brpc)

## Getting Started

Supported operating system: Linux, MacOSX.

* Install [brpc](https://github.com/brpc/brpc/blob/master/docs/cn/getting_started.md)  which is the main dependency of media-server
* Compile media-server with cmake:
```shell
mkdir build && cd build && cmake .. && make -sj4
```
* Run media-server as origin server with minimum configuration(the default port is 8079):
```shell
./output/bin/media_server
```
Then you can push/play stream from it.

### Main options

Please run 
```
./output/bin/media_server --help
```
to get all configurations in detail.

* -proxy_to
When not specified or empty, media-server runs in origin mode, which aggregates push(such as OBS, ffmpeg) and accepts play(such as cyberplayer, ffplay) requests.

* -proxy_lb
When -proxy_to is a naming service(such as http://...), you need to specify load balancing algorithm. The options are rr, random, la, c_murmurhash and c_md5. Read [client load balancing](https://github.com/brpc/brpc/blob/master/docs/en/client.md#user-content-load-balancer) for details.

* -port
Specifies the service port of media-server. Brpc is characterized by supporting all protocols on the same port, so this port can also be used for accessing the built-in service via http. Only ports in the range of 8000-9000 can be accessed by browsers, which means if the service port is external, be sure to configure -internal_port to prevent built-in service from leaking detailed service information.

* -internal_port
This port can be configured as a port that can only be accessed on the internal network. In this case, the -port port no longer provides built-in services, but will only be accessible through this port.

* -retry_interval_ms
When media-server runs in edge mode, push and pull requests to upstreams will be retried when error happens until clients no longer need. This option specifies the minimum interval for continuous retry, which is 1 second by default.

* -share_play_connection
When set to true, multiple streams connected to the same server will reuse the same rtmp connection in play.

* -share_publish_connection
When set to true, multiple streams connected to the same server will reuse the same rtmp connection in publish.

* -timeout_ms
Timeout period for creating a stream when media-server runs in edge mode. The default value is 1000ms.

* -server_idle_timeout
Connections without data transmission for so many seconds will be closed. The default value is -1(turned off).

* -cdn_merge_to
When this option is set, media-server starts two ports, one for external service request and the other for the aggregating request. Usually the aggregating server will be found using consistent hashing, which is used widely in cache service. This option is often used in cdn nodes.

* -cdn_merge_lb
The load balancing algorithm. Read the explanation written below -proxy_lb.

* -flagfile
media-server uses gflags options, which is specified by default in the command line and can also in file format during online deployment by using -flagfile.

## Examples

* Run media-server as [origin server](docs/cn/origin_server.md) and [edge server](docs/cn/edge_server.md).

## Other docs

* Tools
    * [puller](docs/cn/puller.md)
    * [pusher](docs/cn/pusher.md)
    * [random_test](docs/cn/random_test.md)
    * [rtmp_press](docs/cn/rtmp_press.md)
