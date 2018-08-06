media-server的内置http页面提供了查看每条流状态的服务，如下图所示。从左至右分别是创建时间、流的来源、流的key、GOP(Group of pictures)、每秒帧数、每秒视频字节、每秒音频字节、当前播放数、当前流畅度、三种协议的播放测试（使用cyberplayer）。在这个页面中，也可以看到由brpc提供的[内置服务](https://github.com/brpc/brpc/blob/master/docs/cn/builtin_service.md)。

![media_server](../images/media_server.png)
