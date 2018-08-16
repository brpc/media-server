## rtmp简化握手

rtmp协议涉及一个握手的过程，而这个握手的过程会对直播流产生额外的首屏时间。我们发现，如果上下游都用media-server的话，那么这个握手过程完全可以省略，达到的首屏效果完全和http flv相同。

可以通过`-simplified_rtmp_play=true`来打开这个功能，当前的media-server回源下一级media-server时，将使用简化握手的rtmp协议。

注意，真实客户和第一级media-server通信时，还是会自动使用正常的rtmp协议，无论是否打开这个选项，理由是客户端的rtmp实现一般是无法控制和修改的。
