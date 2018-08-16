## 流状态的打印

media-server支持定期打印流的状态到日志中，以实现监控/计费等功能，通过选项`-dump_stats_interval`来打开，例如设置成5(s)，则打开日志打印功能；只要这个选择大于0，则计费日志总是会打开。

### 监控日志

打印的内容和`/media_server`页面上看到的一致，具体解释如下。

* CreatedTime：流的创建时间
* From：流的来源IP
* Key：流的唯一标识，用vhost/app/stream_name来表示
* Gop：Group of pictures的大小
* F/s：帧/秒
* Video B/s：视频帧的字节数/秒
* Audio B/s：音频帧的字节数/秒
* Fluency：当前的流畅度
* Play：当前播放数

### 计费日志

格式如下。
* 日志开始：{start|stop|on}_{flv|hls}_{play|publish}。在开始/结束时，打一个start/stop日志，在拉流/推流中，打印on日志
* key：流的唯一标识，用vhost/app/stream_name来表示
* request：请求ID
* send：发送字节总数
* recv：接收字节总数
* type：时间类型，可选项为internal，external代表内部回源和外部回源
* endpoint：对端IP
