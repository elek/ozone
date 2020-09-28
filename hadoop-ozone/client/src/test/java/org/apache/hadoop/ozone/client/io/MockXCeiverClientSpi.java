package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData.Builder;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

public class MockXCeiverClientSpi extends XceiverClientSpi {

  private Pipeline pipeline;

  private ByteString chunkData;

  public MockXCeiverClientSpi(Pipeline pipeline) {
    this.pipeline = pipeline;
    chunkData = ByteString.copyFrom(new byte[4096 * 1024]);
  }

  @Override
  public void connect() throws Exception {

  }

  @Override
  public void connect(String encodedToken) throws Exception {

  }

  @Override
  public void close() {

  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public XceiverClientReply sendCommandAsync(ContainerCommandRequestProto request)
      throws IOException, ExecutionException, InterruptedException {
    if (request.getCmdType() == Type.ReadChunk) {
      final long len = request.getReadChunk().getChunkData().getLen();
      final long offset = request.getReadChunk().getChunkData().getOffset();
      final ReadChunkRequestProto readChunk = request.getReadChunk();
      return new XceiverClientReply(CompletableFuture
          .completedFuture(ContainerCommandResponseProto.newBuilder()
              .setCmdType(request.getCmdType())
              .setResult(Result.SUCCESS)
              .setReadChunk(ReadChunkResponseProto.newBuilder()
                  .setBlockID(readChunk.getBlockID())
                  .setData(chunkData.substring(0, (int) len))
                  .setChunkData(ChunkInfo.newBuilder()
                      .setLen(len)
                      .setOffset(offset)
                      .setChecksumData(ChecksumData.newBuilder()
                          .setType(ChecksumType.NONE)
                          .setBytesPerChecksum(0)
                          .build())
                      .setChunkName("chunk")
                      .build())
                  .build())
              .build()));
    }
    if (request.getCmdType() == Type.GetBlock) {
      final Builder blockData = BlockData.newBuilder()
          .setBlockID(request.getGetBlock().getBlockID());

      for (int i = 0; i < 64; i++) {
        blockData.addChunks(ChunkInfo.newBuilder()
            .setChunkName("chunk_" + i)
            .setLen(4096 * 1024)
            .setOffset(i * 4096 * 1024)
            .setChecksumData(ChecksumData.newBuilder()
                .setType(ChecksumType.NONE)
                .setBytesPerChecksum(0)
                .build())
            .build());
      }

      return new XceiverClientReply(CompletableFuture
          .completedFuture(ContainerCommandResponseProto.newBuilder()
              .setCmdType(request.getCmdType())
              .setResult(Result.SUCCESS)
              .setGetBlock(GetBlockResponseProto.newBuilder()
                  .setBlockData(blockData)
                  .build())
              .build()));
    }
    System.out.println(request.getCmdType());
    return new XceiverClientReply(CompletableFuture
        .completedFuture(ContainerCommandResponseProto.newBuilder()
            .setCmdType(request.getCmdType())
            .build()));
  }

  @Override
  public ReplicationType getPipelineType() {
    return ReplicationType.RATIS;
  }

  @Override
  public XceiverClientReply watchForCommit(long index)
      throws InterruptedException, ExecutionException, TimeoutException,
      IOException {
    return new XceiverClientReply(CompletableFuture
        .completedFuture(ContainerCommandResponseProto.newBuilder()
            .setCmdType(Type.WriteChunk)
            .build()));
  }

  @Override
  public long getReplicatedMinCommitIndex() {
    return 0;
  }

  @Override
  public Map<DatanodeDetails, ContainerCommandResponseProto> sendCommandOnAllNodes(
      ContainerCommandRequestProto request
  ) throws IOException, InterruptedException {
    return null;
  }
}
