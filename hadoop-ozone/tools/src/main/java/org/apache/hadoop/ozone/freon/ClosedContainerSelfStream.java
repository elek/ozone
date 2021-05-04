/*
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

import com.codahale.metrics.Timer;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.replication.*;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Utility to replicated closed container with datanode code.
 */
@Command(name = "css",
    aliases = "container-self-stream",
    description = "Start a container replicator server and client and tries to stream as much data as possible",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class ClosedContainerSelfStream extends BaseFreonGenerator implements
    Callable<Void> {

  private ReplicationSupervisor supervisor;

  private Timer timer;
  private ReplicationServer replicationServer;
  private ContainerReplicator replicator;

  @Override
  public Void call() throws Exception {

    OzoneConfiguration conf = createOzoneConfiguration();


    //server
    replicationServer = new ReplicationServer(new ClosedContainerStreamGenerator(), new ReplicationServer.ReplicationConfig(), null, null);
    replicationServer.start();


    //client
    initializeReplicationSupervisor(conf);

    init();

    timer = getMetrics().timer("stream-container");
    runTests(this::replicateContainer);
    replicationServer.stop();
    return null;
  }


  /**
   * Check id target directory is not re-used.
   */
  private void checkDestinationDirectory(String dirUrl) throws IOException {
    final StorageLocation storageLocation = StorageLocation.parse(dirUrl);
    final Path dirPath = Paths.get(storageLocation.getUri().getPath());

    if (Files.notExists(dirPath)) {
      return;
    }

    if (Files.list(dirPath).count() == 0) {
      return;
    }

    throw new IllegalArgumentException(
        "Configured storage directory " + dirUrl
            + " (used as destination) should be empty");
  }

  @NotNull
  private void initializeReplicationSupervisor(ConfigurationSource conf)
      throws IOException {
    String fakeDatanodeUuid = UUID.randomUUID().toString();

    ContainerSet containerSet = new ContainerSet();

    MutableVolumeSet volumeSet = new MutableVolumeSet(fakeDatanodeUuid, conf);

    final UUID scmId = UUID.randomUUID();

    replicator = new DownloadAndDiscardReplicator(new SimpleContainerDownloader(conf, null));
    supervisor = new ReplicationSupervisor(containerSet, replicator, 10);
  }

  private void replicateContainer(long counter) throws Exception {
    timer.time(() -> {
      List<DatanodeDetails> datanodes = new ArrayList<>();
      datanodes.add(DatanodeDetails.newBuilder()
          .setHostName("localhost")
          .setIpAddress("127.0.0.1")
          .setUuid(UUID.randomUUID())
          .addPort(DatanodeDetails.newPort(DatanodeDetails.Port.Name.REPLICATION, replicationServer.getPort()))
          .build());
      final ReplicationTask replicationTask =
          new ReplicationTask(counter, datanodes);

      replicator.replicate(replicationTask);
      return null;
    });
  }
}