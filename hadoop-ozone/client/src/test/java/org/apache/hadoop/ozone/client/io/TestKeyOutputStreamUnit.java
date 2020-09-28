package org.apache.hadoop.ozone.client.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.Builder;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;

import org.junit.Test;

public class TestKeyOutputStreamUnit {

  private int blocks = 10;

  public static DatanodeDetails createDatanodeDetails(UUID uuid) {
    Random random = ThreadLocalRandom.current();
    String ipAddress = random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256);
    return createDatanodeDetails(uuid.toString(), "localhost" + "-" + ipAddress,
        ipAddress, null);
  }

  public static DatanodeDetails createDatanodeDetails(
      String uuid,
      String hostname, String ipAddress, String networkLocation
  ) {
    return createDatanodeDetails(uuid, hostname, ipAddress, networkLocation, 0);
  }

  public static DatanodeDetails createDatanodeDetails(
      String uuid,
      String hostname, String ipAddress, String networkLocation, int port
  ) {
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, port);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, port);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, port);
    return DatanodeDetails.newBuilder()
        .setUuid(UUID.fromString(uuid))
        .setHostName(hostname)
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort)
        .setNetworkLocation(networkLocation)
        .build();
  }

  @Test
  public void read() throws IOException {

    List<DatanodeDetails> datanodes = new ArrayList<>();
    datanodes.add(createDatanodeDetails(UUID.randomUUID()));
    datanodes.add(createDatanodeDetails(UUID.randomUUID()));
    datanodes.add(createDatanodeDetails(UUID.randomUUID()));
    final Pipeline pipeline = new Builder()
        .setId(PipelineID.randomId())
        .setFactor(ReplicationFactor.THREE)
        .setType(ReplicationType.RATIS)
        .setState(PipelineState.OPEN)
        .setNodes(datanodes)
        .setLeaderId(datanodes.get(0).getUuid())
        .build();

    ArrayList<OmKeyLocationInfo> keyLocation = new ArrayList<>();
    for (int i = 0; i < blocks; i++) {
      keyLocation.add(new OmKeyLocationInfo.Builder()
          .setLength(256 * 1024 * 1024)
          .setOffset(0)
          .setBlockID(new BlockID(i, 1L))
          .setPipeline(pipeline)
          .build());
    }

    ArrayList<OmKeyLocationInfoGroup> keyLocationInfoGroups = new ArrayList<>();
    keyLocationInfoGroups.add(new OmKeyLocationInfoGroup(0L, keyLocation));

    OmKeyInfo key = new OmKeyInfo.Builder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName("key1")
        .setReplicationFactor(ReplicationFactor.THREE)
        .setReplicationType(ReplicationType.RATIS)
        .setOmKeyLocationInfos(
            keyLocationInfoGroups
        )
        .build();

    LengthInputStream kis = KeyInputStream.getFromOmKeyInfo(key,
        new MockXCeiverManager(),
        false,
        omKeyInfo -> omKeyInfo);

    int bufferSize = 1024;
    byte[] data = new byte[bufferSize];

    final long iterations = blocks * 256l * 1024 * 1024 / bufferSize;
    for (int i = 0; i < iterations; i++) {
      System.out.println("read " + kis.read(data));
    }

  }
}