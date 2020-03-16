/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.common.Checksum;

import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Data generator to use pure datanode XCeiver interface.
 */
@Command(name = "dcg",
    aliases = "datanode-chunk-generator",
    description = "Create as many chunks as possible with pure XCeiverClient.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class DatanodeChunkGenerator extends BaseFreonGenerator implements
    Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeChunkGenerator.class);

  @Option(names = {"-s", "--size"},
      description = "Size of the generated chunks (in bytes)",
      defaultValue = "1024")
  private int chunkSize;

  @Option(names = {"-l", "--pipeline"},
      description = "Pipeline to use. By default the first RATIS/THREE "
          + "pipeline will be used.",
      defaultValue = "")
  private String pipelineId;

  @Option(names = {"--watch-commit"},
      description = "Watch commit after every time after this number of write"
          + " chunk (0=never)",
      defaultValue = "0")
  private int watchCommitPerChunk;

  @Option(names = {"--chunk-per-block"},
      description = "Number of chunks in one block.",
      defaultValue = "1")
  private long chunkPerBlock;

  private long blockPerContainer;

  private XceiverClientSpi xceiverClientSpi;

  private Timer timer;

  private ByteString dataToWrite;

  private ChecksumData checksumProtobuf;


  private long maxIndex;

  @Override
  public Void call() throws Exception {

    init();

    OzoneConfiguration ozoneConf = createOzoneConfiguration();

    long containerSize =
        (long) ozoneConf.getStorageSize(OZONE_SCM_CONTAINER_SIZE,
            OZONE_SCM_CONTAINER_SIZE_DEFAULT,
            StorageUnit.BYTES);

    long blockSize = (long) ozoneConf
        .getStorageSize(OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT,
            StorageUnit.BYTES);

    blockPerContainer = (long) (containerSize * 0.8) / chunkSize;
    chunkPerBlock = blockSize / chunkSize;

    if (OzoneSecurityUtil.isSecurityEnabled(ozoneConf)) {
      throw new IllegalArgumentException(
          "Datanode chunk generator is not supported in secure environment");
    }

    try (StorageContainerLocationProtocol scmLocationClient =
        createStorageContainerLocationClient(ozoneConf)) {
      List<Pipeline> pipelines = scmLocationClient.listPipelines();
      Pipeline pipeline;
      if (pipelineId != null && pipelineId.length() > 0) {
        pipeline = pipelines.stream()
            .filter(p -> p.getId().toString().equals(pipelineId))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(
                "Pipeline ID is defined, but there is no such pipeline: "
                    + pipelineId));

      } else {
        pipeline = pipelines.stream()
            .filter(p -> p.getFactor() == ReplicationFactor.THREE)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(
                "Pipeline ID is NOT defined, and no pipeline " +
                    "has been found with factor=THREE"));
        LOG.info("Using pipeline {}", pipeline.getId());
      }

      try (XceiverClientManager xceiverClientManager =
               new XceiverClientManager(ozoneConf)) {
        xceiverClientSpi = xceiverClientManager.acquireClient(pipeline);

        byte[] data = RandomStringUtils.randomAscii(chunkSize)
            .getBytes(StandardCharsets.UTF_8);

        dataToWrite = ByteString.copyFrom(data);

        Checksum checksum = new Checksum(ChecksumType.CRC32, chunkSize);
        checksumProtobuf = checksum.computeChecksum(data).getProtoBufMessage();

        timer = getMetrics().timer("chunk-write");

        runTests(this::writeChunks);

        //wait until all the messages are committed
        xceiverClientSpi.watchForCommit(maxIndex);
      }
    } finally {
      if (xceiverClientSpi != null) {
        xceiverClientSpi.close();
      }
    }
    return null;
  }

  private void writeChunks(long stepNo)
      throws Exception {

    long containerId = stepNo / blockPerContainer + 1;
    long blockLocalId = stepNo + 1;

    for (int i = 0; i < chunkPerBlock; i++) {
      DatanodeBlockID blockId = DatanodeBlockID.newBuilder()
          .setContainerID(containerId)
          .setLocalID(blockLocalId)
          .setBlockCommitSequenceId(stepNo)
          .build();

      ChunkInfo chunkInfo = ChunkInfo.newBuilder()
          .setChunkName(getPrefix() + "_testdata_chunk_" + stepNo)
          .setOffset(i * chunkSize)
          .setLen(chunkSize)
          .setChecksumData(checksumProtobuf)
          .build();

      WriteChunkRequestProto.Builder writeChunkRequest =
          WriteChunkRequestProto
              .newBuilder()
              .setBlockID(blockId)
              .setChunkData(chunkInfo)
              .setData(dataToWrite);

      String id = xceiverClientSpi.getPipeline().getFirstNode().getUuidString();

      ContainerCommandRequestProto.Builder builder =
          ContainerCommandRequestProto
              .newBuilder()
              .setCmdType(Type.WriteChunk)
              .setContainerID(blockId.getContainerID())
              .setDatanodeUuid(id)
              .setWriteChunk(writeChunkRequest);

      ContainerCommandRequestProto request = builder.build();

      timer.time(() -> {
        XceiverClientReply xceiverClientReply =
            xceiverClientSpi.sendCommandAsync(request);
        maxIndex = Math.max(xceiverClientReply.getLogIndex(), maxIndex);
        return null;
      });
      if ((watchCommitPerChunk > 0) && ((i + 1) % watchCommitPerChunk == 0)) {
        xceiverClientSpi.watchForCommit(maxIndex);
      }
    }

  }

}
