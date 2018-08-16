## 重试策略

在代理模式下，有三个选项可以用来控制拉流和推流的重试。

* -fast_retry_count
当推流/拉流发生异常时，会进行-fast_retry_count次立即重试，适用于server重启、连接丢失等场景。

* -retry_interval_ms
当fast retry阶段结束后，连续重试的间隔，默认值为1000毫秒。

* -max_retry_duration_ms
一次重试持续的最大时长，默认为10000毫秒。
