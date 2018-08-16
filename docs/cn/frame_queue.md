## 帧队列缓冲（frame queue buffer）

在media-server中，有两种控制每条流的音视频帧缓冲的方法。

### min_cache_length(ms)

返回给新用户的第一帧是media-server的帧缓冲中，与当前最新的帧相隔大于`min_cache_length`（单位毫秒）时间长度的所有音视频帧中最新的帧。例如，如果这个值设置为2000ms，那么media-server会返回给新用户2000ms前的第一个关键帧开始的所有数据，这个缓冲队列可以用来缓解卡顿。

### -max_cache_length(ms)

返回给新用户的第一帧是media-server的帧缓冲中，与当前最新的帧相隔小于`max_cache_length`（单位毫秒）时间长度的所有音视频帧中最老的帧。例如，如果这个值设置为2000ms，那么media-server会返回给新用户2000ms内的最老的关键帧开始的所有数据，这个缓冲队列可以用来控制最大延迟。

默认情况下，media-server会使用min cache来作为缓冲策略，可以用`-min_buffering_ms`来为所有的流设置默认缓冲大小。

## 帧指示（FrameIndicator）

在不同的客户场景下，希望采取不同的缓存策略，media-server提供了如下两种帧指示来为每条流单独设置min_cache_length和max_cache_length

### FrameIndicator::BY_USING_LATEST

总是返回最新的帧。这个Indicator常用在实时性较高的场合中，例如互动直播。注意，当启用时，由于返回给客户端的第一帧不一定是关键帧，所以会出现小间隔的黑屏现象。

### FrameIndicator::BY_CONFIG

根据配置设置cache_length，这个配置可以由中心业务服务器统一下发给media-server。
