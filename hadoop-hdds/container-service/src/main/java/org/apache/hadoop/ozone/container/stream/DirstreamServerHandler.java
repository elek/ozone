package org.apache.hadoop.ozone.container.stream;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultFileRegion;
import io.netty.util.ByteProcessor;

public class DirstreamServerHandler extends ChannelInboundHandlerAdapter {

    private StreamingSource source;

    private final StringBuilder id = new StringBuilder();
    private boolean headerProcessed = false;

    public DirstreamServerHandler(StreamingSource source) {
        this.source = source;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {
        if (!headerProcessed) {
            ByteBuf buffer = (ByteBuf) msg;
            int eolPosition = buffer.forEachByte(ByteProcessor.FIND_LF) - buffer
                .readerIndex();
            if (eolPosition > 0) {
                headerProcessed = true;
                id.append(buffer.readBytes(eolPosition)
                    .toString(Charset.defaultCharset()));
            } else {
                id.append(buffer.toString(Charset.defaultCharset()));
            }
        }

        if (headerProcessed) {
            ChannelFuture future = null;

            for (Map.Entry<String, Path> entries : source
                .getFilesToStream(id.toString())
                .entrySet()) {
                Path file = entries.getValue();

                String name = entries.getKey();
                long fileSize = Files.size(file);
                String identifier = fileSize + " " + name + "\n";
                ByteBuf identifierBuf =
                    Unpooled.wrappedBuffer(identifier.getBytes(
                        StandardCharsets.UTF_8));

                if (future == null) {
                    future = ctx.writeAndFlush(identifierBuf)
                        .addListener(f -> ctx.writeAndFlush(
                            new DefaultFileRegion(file.toFile(), 0, fileSize)));

                } else {
                    future.addListener(f -> ctx.writeAndFlush(identifierBuf))
                        .addListener(f -> ctx.writeAndFlush(
                            new DefaultFileRegion(file.toFile(), 0, fileSize)));

                }
            }

            future.addListener(f -> ctx.channel().close());
        }
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
