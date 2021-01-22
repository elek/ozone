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

import java.util.List;
import java.util.function.Supplier;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_CHOOSING_POLICY;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default replication implementation.
 * <p>
 * This class does the real job. Executes the download and import the container
 * to the container set.
 */
public class DownloadAndImportReplicator implements ContainerReplicator {

  public static final Logger LOG =
      LoggerFactory.getLogger(DownloadAndImportReplicator.class);

  private final ContainerSet containerSet;

  private final ContainerDownloader downloader;
  private final ConfigurationSource config;

  private final Supplier<String> scmId;

  private VolumeSet volumeSet;

  public DownloadAndImportReplicator(
      ConfigurationSource config,
      Supplier<String> scmId,
      ContainerSet containerSet,
      ContainerDownloader downloader,
      VolumeSet volumeSet
  ) {
    this.containerSet = containerSet;
    this.downloader = downloader;
    this.config = config;
    this.scmId = scmId;
    this.volumeSet = volumeSet;
  }


  @Override
  public void replicate(ReplicationTask task) {
    long containerID = task.getContainerId();
    if (scmId.get() == null) {
      LOG.error("Replication task is called before first SCM call");
      task.setStatus(Status.FAILED);
    }
    List<DatanodeDetails> sourceDatanodes = task.getSources();

    LOG.info("Starting replication of container {} from {}", containerID,
        sourceDatanodes);

    try {

      VolumeChoosingPolicy volumeChoosingPolicy = config.getClass(
          HDDS_DATANODE_VOLUME_CHOOSING_POLICY, RoundRobinVolumeChoosingPolicy
              .class, VolumeChoosingPolicy.class).newInstance();

      long maxContainerSize = (long) config.getStorageSize(
          ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
          ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

      KeyValueContainerData containerData =
          new KeyValueContainerData(containerID,
              ChunkLayOutVersion.FILE_PER_BLOCK, maxContainerSize, "", "");

      final KeyValueContainerData loadedContainerData =
          downloader
              .getContainerDataFromReplicas(containerData, sourceDatanodes);

      final HddsVolume volume = volumeChoosingPolicy
          .chooseVolume(volumeSet.getVolumesList(), maxContainerSize);
      loadedContainerData.assignToVolume(scmId.get(), volume);

      //write out container descriptor
      KeyValueContainer keyValueContainer =
          new KeyValueContainer(loadedContainerData, config);

      //rewriting the yaml file with new checksum calculation.
      keyValueContainer.update(loadedContainerData.getMetadata(), true);

      //fill in memory stat counter (keycount, byte usage)
      KeyValueContainerUtil.parseKVContainerData(containerData, config);

      //load container
      containerSet.addContainer(keyValueContainer);

      LOG.info("Container {} is downloaded, starting to import.",
          containerID);

      LOG.info("Container {} is replicated successfully", containerID);
      task.setStatus(Status.DONE);
    } catch (Exception e) {
      LOG.error("Container {} replication was unsuccessful.", containerID, e);
      task.setStatus(Status.FAILED);
    }
  }
}
