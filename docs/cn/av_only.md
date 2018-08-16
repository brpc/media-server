## 纯音频/视频播放

### flv

在http url的请求字符串中加入`only-audio=1`或者`only-video=1`即可开启纯音频或者纯视频流。若两个请求同时设置，则全部忽略。

### rtmp

在Play Command的StreamName中可以用类似请求字符串的方式来决定是否开启，开启方式和flv类似，例如"stream_name?only-audio=1"即开启纯音频流。
