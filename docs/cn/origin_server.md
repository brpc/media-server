## 运行media-server作为源站模式

media-server可以在源站模式和代理模式下工作，这两种模式的区别在于，代理模式下会将推流/拉流请求转发给指定的下一级server，而源站模式下，推流/拉流不会再做转发，此时如果拉的流并没有推到这个server，就会发生错误。

`conf/origin.conf`是源站模式下的配置示例，可以用如下方式启动源站：

```shell
./media_server --flagfile=conf/origin.conf
```

如果正常启动，会看到如下启动日志：

> Server[media_server] is serving on port=8777 and internal_port=8222.
> Check out http://127.0.0.1:8222 in web browser.

日志的第二行提示了media-server的HTTP页面，可以用来观测server目前的状态，更详细的介绍请看[这里](http_service.md)。

通过推流工具（例如ffmpeg，或media-server提供的tools中的pusher）将流推到127.0.0.1:8777，然后使用第三方播放器或者media-server的HTTP页面的播放入口（使用cyberplayer）进行播放。
