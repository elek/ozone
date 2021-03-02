package org.apache.hadoop.ozone.container.stream;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ByteProcessor;
import io.netty.util.ReferenceCountUtil;

public class DirstreamClientHandler extends ChannelInboundHandlerAdapter {

    private final StreamingDestination destination;
    private boolean headerMode = true;
    private StringBuilder currentFileName = new StringBuilder();
    private RandomAccessFile destFile;

    private FileChannel destFileChannel;

    private long remaining;

    public DirstreamClientHandler(StreamingDestination streamingDestination) {
        this.destination = streamingDestination;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws IOException {
        try {
            ByteBuf buffer = (ByteBuf) msg;
            doRead(ctx, buffer);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    public void doRead(ChannelHandlerContext ctx, ByteBuf buffer)
        throws IOException {
        if (headerMode) {
            int eolPosition = buffer.forEachByte(ByteProcessor.FIND_LF) - buffer
                .readerIndex();
            if (eolPosition > 0) {
                headerMode = false;
                currentFileName.append(buffer.readBytes(eolPosition)
                    .toString(StandardCharsets.UTF_8));
                buffer.readBytes(1);
                String[] parts = currentFileName.toString().split(" ", 2);
                remaining = Long.parseLong(parts[0]);
                Path destFilePath = destination.mapToDestination(parts[1]);
                Files.createDirectories(destFilePath.getParent());
                this.destFile =
                    new RandomAccessFile(destFilePath.toFile(), "rw");
                destFileChannel = this.destFile.getChannel();
            } else {
                currentFileName
                    .append(buffer.toString(StandardCharsets.UTF_8));
            }
        }
        if (!headerMode) {
            if (remaining > buffer.readableBytes()) {
                remaining -=
                    buffer.readBytes(destFileChannel, buffer.readableBytes());
            } else {
                remaining -= buffer.readBytes(destFileChannel, (int) remaining);
                currentFileName = new StringBuilder();
                headerMode = true;
                if (buffer.readableBytes() > 0) {
                    doRead(ctx, buffer);
                }
            }
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
        try {
            if (destFile != null) {
                destFile.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        try {
            destFileChannel.close();
            destFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ctx.close();
    }

}
