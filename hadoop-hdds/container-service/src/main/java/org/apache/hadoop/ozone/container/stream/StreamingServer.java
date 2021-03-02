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
import io.netty.handler.stream.ChunkedWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingServer {

    private static final Logger LOG =
        LoggerFactory.getLogger(StreamingServer.class);

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
        bossGroup = new NioEventLoopGroup(100);
        workerGroup = new NioEventLoopGroup(100);

        b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 100)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(
                        new ChunkedWriteHandler(),
                        new DirstreamServerHandler(source));
                }
            });

        ChannelFuture f = b.bind(port).sync();
        final InetSocketAddress socketAddress =
            (InetSocketAddress) f.channel().localAddress();
        port = socketAddress.getPort();
        LOG.info("Started streaming server on " + port);
    }

    public void stop() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    public int getPort() {
        return port;
    }
}
