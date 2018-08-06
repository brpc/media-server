## random_test

random_test是一个工具，用于模拟大量的随机推流和随机拉流，一般用于播放集群的压力测试，检测服务器是否有异常状态。使用示例：
```
random_test --push_server=127.0.0.1:8906 --play_server=127.0.0.1:8905 --test_duration_s=-1
```

### 主要选项

* -play_server
拉流的地址，必须指定。

* -push_server
推流的地址，必须指定。

* -max_stream_num & -min_stream_num
存在推流数量的最大和最小范围，默认值为80和30。

* -fps
推流的每秒帧数，默认值为30。

* -frame_size
每一帧的大小，默认值为4096字节。

* -test_duration_s
随机推拉流持续的时间，默认值为20秒，设置为-1表示一直跑下去。
