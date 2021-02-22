package org.apache.hadoop.ozone.container.stream;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultFileRegion;

public class DirstreamServerHandler extends ChannelInboundHandlerAdapter {

    private StreamingSource source;

    public DirstreamServerHandler(StreamingSource source) {
        this.source = source;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
        ChannelFuture future = null;

        for (Map.Entry<String, Path> entries : source.getFilesToStream("1")
            .entrySet()) {
            Path file = entries.getValue();
            String name = entries.getKey();

            if (future == null) {
                long fileSize = Files.size(file);
                future = ctx.writeAndFlush(
                    fileSize + " " + name + "\n")
                    .addListener(f -> ctx.writeAndFlush(
                        new DefaultFileRegion(file.toFile(), 0, fileSize)));

            } else {
                long fileSize = Files.size(file);
                future.addListener(f -> ctx.writeAndFlush(
                    fileSize + " " + name + "\n"))
                    .addListener(f -> ctx.writeAndFlush(
                        new DefaultFileRegion(file.toFile(), 0, fileSize)));

            }
        }

        future.addListener(f -> ctx.channel().close());
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
        cause.printStackTrace();
        if (ctx.channel().isActive()) {
            ctx.writeAndFlush("ERR: " +
                cause.getClass().getSimpleName() + ": " +
                cause.getMessage() + '\n').addListener(
                ChannelFutureListener.CLOSE);
        }
        ctx.close();
    }
}
