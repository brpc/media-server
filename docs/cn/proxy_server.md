## 运行media-server作为代理模式

media-server可以在源站模式和代理模式下工作，这两种模式的区别在于，代理模式下会将推流/拉流请求转发给指定的下一级server，而源站模式下，推流/拉流不会再做转发，此时如果拉的流并没有推到这个server，就会发生错误。

`conf/proxy.conf`是源站模式下的配置示例，可以用如下方式启动源站：

```shell
./media_server --flagfile=conf/proxy.conf
```

如果正常启动，会看到如下启动日志：

> Server[media_server] is serving on port=8955 and internal_port=8954.
> Check out http://BDSHYF000088024:8954 in web browser.
> Server[media_server(cdn-merge)] is serving on port=7035.
> Check out http://BDSHYF000088024:7035 in web browser.

由于启动了两个service（接受请求的service和merge service），相关的日志打印了两遍。

通过推流工具（例如ffmpeg，或media-server提供的tools中的pusher）将流推到127.0.0.1:8955，此时media-server会将这条流推到配置文件中proxy_to指定的地址去。拉流也是类似的，会将拉流请求代理到proxy_to指定的地址。
