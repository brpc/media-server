## rtmp_press

rtmp_press是一个工具，用于检测拉流的正确性，即从media-server拉到的音视频数据是否不遗漏不重复。使用示例：
```
rtmp_press --push_server=127.0.0.1:8906 --play_server=127.0.0.1:8905 --push_vhost=test.com --play_vhost=test.com
```

### 主要选项

* -match_duration
检测持续时间，默认值为10秒，设置为-1表示一直跑下去。

* -play_server
拉流的地址，必须指定。

* -push_server
推流的地址，必须指定。

* -push_vhost
推流的vhost，若不指定，则用push_server作为默认值。

* -play_vhost
拉流的vhost，若不指定，则用play_server作为默认值。

* -stream_num
并发的流的数量，默认为1。

* -player_num
对于每条流，播放个数，默认为1。
