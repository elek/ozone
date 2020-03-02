package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

public class ContainerProtocolUtils {

  /**
   * Calls the container protocol to get a container block.
   *
   * @param xceiverClient   client to perform call
   * @param datanodeBlockID blockID to identify container
   * @return container protocol get block response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static GetBlockResponseProto getBlock(XceiverClientSpi xceiverClient,
      DatanodeBlockID datanodeBlockID) throws IOException {
    GetBlockRequestProto.Builder readBlockRequest = GetBlockRequestProto
        .newBuilder()
        .setBlockID(datanodeBlockID);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetBlock)
        .setContainerID(datanodeBlockID.getContainerID())
        .setDatanodeUuid(id)
        .setGetBlock(readBlockRequest);
    String encodedToken = getEncodedBlockToken(getService(datanodeBlockID));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }

    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response =
        xceiverClient
            .sendCommand(request, ContainerProtocolCalls.getValidatorList());
    if (ContainerProtos.Result.CONTAINER_NOT_FOUND.equals(
        response.getResult())) {
      throw new ContainerNotFoundException(response.getMessage());
    }
    return response.getGetBlock();
  }

  /**
   * Returns a url encoded block token. Service param should match the service
   * field of token.
   *
   * @param service
   */
  private static String getEncodedBlockToken(Text service)
      throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Token<OzoneBlockTokenIdentifier> token =
        OzoneBlockTokenSelector.selectBlockToken(service, ugi.getTokens());
    if (token != null) {
      return token.encodeToUrlString();
    }
    return null;
  }

  private static Text getService(DatanodeBlockID blockId) {
    return new Text(new StringBuffer()
        .append("conID: ")
        .append(blockId.getContainerID())
        .append(" locID: ")
        .append(blockId.getLocalID())
        .toString());
  }

  /**
   * Calls the container protocol to put a container block.
   *
   * @param xceiverClient      client to perform call
   * @param containerBlockData block data to identify container
   * @return putBlockResponse
   * @throws IOException          if there is an error while performing the call
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public static XceiverClientReply putBlockAsync(
      XceiverClientSpi xceiverClient, BlockData containerBlockData)
      throws IOException, InterruptedException, ExecutionException {
    PutBlockRequestProto.Builder createBlockRequest =
        PutBlockRequestProto.newBuilder().setBlockData(containerBlockData);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.PutBlock)
            .setContainerID(containerBlockData.getBlockID().getContainerID())
            .setDatanodeUuid(id)
            .setPutBlock(createBlockRequest);
    String encodedToken =
        getEncodedBlockToken(getService(containerBlockData.getBlockID()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    return xceiverClient.sendCommandAsync(request);
  }

  /**
   * Calls the container protocol to write a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk         information about chunk to write
   * @param blockID       ID of the block
   * @param data          the data of the chunk to write
   * @throws IOException if there is an I/O error while performing the call
   */
  public static XceiverClientReply writeChunkAsync(
      XceiverClientSpi xceiverClient, ChunkInfo chunk, BlockID blockID,
      ByteString data)
      throws IOException, ExecutionException, InterruptedException {
    WriteChunkRequestProto.Builder writeChunkRequest =
        WriteChunkRequestProto.newBuilder()
            .setBlockID(blockID.getDatanodeBlockIDProtobuf())
            .setChunkData(chunk).setData(data);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.WriteChunk)
            .setContainerID(blockID.getContainerID())
            .setDatanodeUuid(id).setWriteChunk(writeChunkRequest);
    String encodedToken = getEncodedBlockToken(new Text(blockID.
        getContainerBlockID().toString()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    return xceiverClient.sendCommandAsync(request);
  }

  /**
   * Calls the container protocol to read a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk         information about chunk to read
   * @param blockID       ID of the block
   * @param validators    functions to validate the response
   * @return container protocol read chunk response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static ContainerProtos.ReadChunkResponseProto readChunk(
      XceiverClientSpi xceiverClient, ChunkInfo chunk, BlockID blockID,
      List<CheckedBiFunction> validators) throws IOException {
    ReadChunkRequestProto.Builder readChunkRequest =
        ReadChunkRequestProto.newBuilder()
            .setBlockID(blockID.getDatanodeBlockIDProtobuf())
            .setChunkData(chunk);
    String id = xceiverClient.getPipeline().getClosestNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.ReadChunk)
            .setContainerID(blockID.getContainerID())
            .setDatanodeUuid(id).setReadChunk(readChunkRequest);
    String encodedToken = getEncodedBlockToken(new Text(blockID.
        getContainerBlockID().toString()));
    if (encodedToken != null) {
      builder.setEncodedToken(encodedToken);
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto reply =
        xceiverClient.sendCommand(request, validators);
    return reply.getReadChunk();
  }

}
