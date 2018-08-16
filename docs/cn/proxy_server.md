## 运行media-server作为代理模式

media-server可以在源站模式和代理模式下工作，这两种模式的区别在于，代理模式下会将推流/拉流请求转发给指定的下一级server，而源站模式下，推流/拉流不会再做转发，此时如果拉的流并没有推到这个server，就会返回错误。

需要注意的是，如果将media-server部署在CDN节点中，当一个节点向另一个节点/源站回源时，我们希望一条流只有一次回源以减小带宽成本。media-server的解决办法是同一个进程会监听两个端口，第一个端口用来服务客户的请求，然后通过[一致性哈希](https://en.wikipedia.org/wiki/Consistent_hashing)的方式找到由第二个端口服务的进程（merge service），由该进程来做真正的回源。

CDN节点内部的一致性哈希机器可以由`-cdn_merge_to`和`-cdn_merge_lb`指定，前者表明机器地址，可以写成名字服务的形式，后者表明选机器时的负载均衡算法。此时，`-proxy_to`和`-proxy_lb`用来设置merge service回源的真正地址，比如源站。

`conf/proxy.conf`是代理模式下的配置示例，可以用如下方式启动源站：

```shell
./media_server --flagfile=conf/proxy.conf
```

如果正常启动，会看到如下启动日志：

> Server[media_server] is serving on port=8955 and internal_port=8954.
> Check out http://127.0.0.1:8954 in web browser.
> Server[media_server(cdn-merge)] is serving on port=7035.
> Check out http://127.0.0.1:7035 in web browser.

由于启动了两个service（接受请求的service和merge service），相关的日志打印了两遍。

通过推流工具（例如ffmpeg，或media-server提供的tools中的pusher）将流推到127.0.0.1:8955，此时media-server会将这条流推到配置文件中proxy_to指定的地址去。拉流也是类似的，会将拉流请求代理到proxy_to指定的地址。
