package org.apache.hadoop.ozone.container.stream;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;

public class StreamingClient {

    private static EventLoopGroup group;
    private final Bootstrap b;
    private ChannelFuture f;
    private int port;
    private String host;
    private StreamingDestination streamingDestination;

    public StreamingClient(
        String host,
        int port,
        StreamingDestination streamingDestination
    ) throws InterruptedException {
        this.port = port;
        this.host = host;
        this.streamingDestination = streamingDestination;

        group = new NioEventLoopGroup();

        b = new Bootstrap();
        b.group(group)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(new LoggingHandler(LogLevel.INFO),
                        new StringEncoder(CharsetUtil.UTF_8),
                        new DirstreamClientHandler(streamingDestination));
                }
            });

    }

    public Channel connect() throws InterruptedException {
        f = b.connect(host, port).sync();
        return f.channel();
    }

    public void close() throws InterruptedException {
        f.channel().closeFuture().sync();
        group.shutdownGracefully();
    }

}
