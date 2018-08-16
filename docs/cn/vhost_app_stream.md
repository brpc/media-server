## vhost/app/stream_name

在media-server中，一条流由三元组(vhost, app, stream_name)唯一确定。

## 推流参数的确定

在rtmp推流中，由如下规则来确定三元组：

1. rtmp createStream时携带tcUrl，例如tcUrl=rtmp://example.baidu.com/example_app，此时的vhost为example.baidu.com，app为example_app
2. 若上述tcUrl中带了请求字符串，例如tcUrl=rtmp://example.baidu.com/example_app?vhost=example2.baidu.com，则vhost被覆盖为请求字符串中的vhost
3. 在rtmp publish时携带的stream_name中，可以通过请求字符串来覆盖之前tcUrl中的vhost和app，例如stream_name?vhost=xxxx&app=bbbbb

## 拉流参数的确定

### rtmp

vhost和app的确定与rtmp推流中的createStream阶段相同，当发送rtmp play指令时可以通过在stream_name后面带上请求字符串vhost=xxxx&app=bbbbb来覆盖之前的vhost和app。

### flv

播放格式：http://domain/app/stream_name.flv?vhost=real_vhost

url后面的vhost是可选项，如果加上，则会覆盖url中的domain作为请求流的vhost。

### hls

播放格式：http://domain/app/stream_name.m3u8?vhost=real_vhost

url后面的vhost是可选项，如果加上，则会覆盖url中的domain作为请求流的vhost。
