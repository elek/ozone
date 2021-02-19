package org.apache.hadoop.ozone.container.stream;

import java.net.InetSocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.CharsetUtil;

public class StreamingServer {

    private int port;

    private StreamingSource source;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    public StreamingServer(
        StreamingSource source, int port
    ) {
        this.port = port;
        this.source = source;
    }

    public void start() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 100)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(
                        new StringEncoder(CharsetUtil.UTF_8),
                        new LineBasedFrameDecoder(8192),
                        new StringDecoder(CharsetUtil.UTF_8),
                        new ChunkedWriteHandler(),
                        new DirstreamServerHandler(source));
                }
            });

        ChannelFuture f = b.bind(port).sync();
        final InetSocketAddress socketAddress =
            (InetSocketAddress) f.channel().localAddress();
        port = socketAddress.getPort();

    }

    public void stop() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    public int getPort() {
        return port;
    }
}
