## puller

puller是一个从指定地址拉多个流的工具，一般用于对代理集群（边沿节点）做压力测试。使用示例：
```
puller --streams="127.0.01:8079/live/hks?vhost=example.com"
```

### 主要选项

* -streams
所有流的stream列表，形如HOST/APP/STREAM1,STREAM2, 可用换行、空格或逗号分隔。

* -share_connection
是否复用连接。如果-server是media-server，设为true，其他均设为false。详见media-server中对-share_connection选项的解释。

* -probe
探查模式。用于查看一个或多个流的启动时间点。-probe_video_messages可设置探查的video message个数，默认10，收到这么多消息后程序会退出。

* -retry_interval_ms
重试的最短间隔，单位毫秒，默认1000毫秒。
