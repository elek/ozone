/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fake replicator for downloading the data and ignoring it.
 * <p>
 * Usable only for testing.
 */
public class DownloadAndDiscardReplicator implements ContainerReplicator {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadAndDiscardReplicator.class);
  private final ContainerDownloader downloader;

  public DownloadAndDiscardReplicator(ContainerDownloader downloader) {
    this.downloader = downloader;
  }

  @Override
  public void replicate(ReplicationTask task) {
    long containerID = task.getContainerId();

    List<DatanodeDetails> sourceDatanodes = task.getSources();

    LOG.info("Starting replication of container {} from {}", containerID,
        sourceDatanodes);

    CompletableFuture<Path> tempTarFile = downloader
        .getContainerDataFromReplicas(containerID,
            sourceDatanodes);
    try {
      final Path containerPath = tempTarFile.get();
      Files.delete(containerPath);
      LOG.info("Container is downloaded but deleted, as you wished " + containerPath);
    } catch (Exception e) {
      LOG.error("Error on downloading container", e);
    }
  }
}
