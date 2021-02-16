package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.test.GenericTestUtils;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Test;

/**
 * Testing end2end replication without datanode.
 */
public class TestReplicationService {

  @Test
  public void test() throws IOException, TimeoutException,
      InterruptedException {
    final UUID scmUuid = UUID.randomUUID();
    //start server
    ConfigurationSource ozoneConfig = new OzoneConfiguration();

    final String sourceDnUUID = UUID.randomUUID().toString();
    final String destDnUUID = UUID.randomUUID().toString();
    MutableVolumeSet sourceVolumes =
        new MutableVolumeSet(sourceDnUUID, ozoneConfig);

    VolumeChoosingPolicy v = new RoundRobinVolumeChoosingPolicy();
    final HddsVolume volume =
        v.chooseVolume(sourceVolumes.getVolumesList(), 5L);

    KeyValueContainerData kvd = new KeyValueContainerData(1L, "/tmp/asd");
    kvd.setState(State.OPEN);
    kvd.assignToVolume(scmUuid.toString(), volume);
    kvd.setSchemaVersion(OzoneConsts.SCHEMA_V2);
    KeyValueContainer kvc = new KeyValueContainer(kvd, ozoneConfig);
    kvc.create(sourceVolumes, v, scmUuid.toString());

    ContainerSet sourceContainerSet = new ContainerSet();
    sourceContainerSet.addContainer(kvc);

    KeyValueHandler handler = new KeyValueHandler(ozoneConfig,
        sourceDnUUID, sourceContainerSet, sourceVolumes,
        new ContainerMetrics(new int[] {}),
        containerReplicaProto -> {
        });

    final ContainerCommandRequestProto containerCommandRequest =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.WriteChunk)
            .setDatanodeUuid(destDnUUID)
            .setContainerID(kvc.getContainerData().getContainerID())
            .setWriteChunk(WriteChunkRequestProto.newBuilder()
                .setBlockID(DatanodeBlockID.newBuilder()
                    .setContainerID(kvc.getContainerData().getContainerID())
                    .setBlockCommitSequenceId(1L)
                    .setLocalID(1L)
                    .build())
                .setData(ByteString.copyFromUtf8("asdf"))
                .setChunkData(ChunkInfo.newBuilder()
                    .setChunkName("chunk1")
                    .setOffset(1L)
                    .setLen(4)
                    .setChecksumData(ChecksumData.newBuilder()
                        .setType(ChecksumType.NONE)
                        .setBytesPerChecksum(16)
                        .build())
                    .build())
                .build())
            .build();

    handler.handle(containerCommandRequest, kvc, new DispatcherContext.Builder().build());

    HashMap<ContainerType, Handler> handlers = Maps.newHashMap();
    ContainerController controller =
        new ContainerController(sourceContainerSet, handlers);

    ReplicationConfig replicationConfig = new ReplicationConfig();
    replicationConfig.setPort(0);

    SecurityConfig securityConfig = new SecurityConfig(ozoneConfig);
    ReplicationServer replicationServer =
        new ReplicationServer(controller, replicationConfig, securityConfig,
            null);

    replicationServer.init();
    replicationServer.start();

    //start client
    OzoneConfiguration clientConfig = new OzoneConfiguration();
    clientConfig.set("hdds.datanode.dir","tmp/qwe");
    MutableVolumeSet volumeSet =
        new MutableVolumeSet(destDnUUID, clientConfig);

    DownloadAndImportReplicator replicator = new DownloadAndImportReplicator(
        ozoneConfig,
        () -> scmUuid.toString(),
        sourceContainerSet,
        new SimpleContainerDownloader(ozoneConfig, null),
        volumeSet);

    DatanodeDetails source =
        DatanodeDetails.newBuilder()
            .setIpAddress("127.0.0.1")
            .setUuid(UUID.randomUUID())
            .build();
    source.setPort(Name.REPLICATION, replicationServer.getPort());
    List<DatanodeDetails> sourceDatanodes = new ArrayList<>();
    sourceDatanodes.add(source);

    ContainerSet destinationContainerSet = new ContainerSet();
    ReplicationSupervisor supervisor =
        new ReplicationSupervisor(destinationContainerSet, replicator, 10);
    replicator.replicate(new ReplicationTask(1L, sourceDatanodes));

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return destinationContainerSet.getContainer(1L) != null;
      }
    }, 1000, 10_000);
  }

}