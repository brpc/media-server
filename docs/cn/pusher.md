## pusher

pusher是一个工具， 可以从指定地址拉多个流并推送到另一个指定地址处，一般用于播放集群的压力测试。

### 主要选项

* -streams
所有待拉的流，形如HOST/APP/STREAM1,STREAM2, 可用换行、空格、逗号分隔。

* -push_to
推流的地址，必须指定。注意这个地址最好不是localhost:8079或127.0.0.1:8079这类，用了不会影响拉流，但会使/media_server页面上的播放链接无法工作（因为访问不到那些地址）。

* -pull_share_conn & -push_share_conn
拉流或推流是否复用连接。如果-server是media-server，设为true，其他均设为false。详见media-server中对共享连接的解释。

* -push_app
推流的app name，默认为拉流的app。

* -push_stream
推流的stream name，默认为拉流的stream。

* -push_vhost
推流的vhost name，默认为拉流的vhost。
