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
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

/**
 * Fake replicator for downloading the data and ignoring it.
 * <p>
 * Usable only for testing.
 */
public class DownloadAndDiscardReplicator extends DownloadAndImportReplicator {

  public DownloadAndDiscardReplicator(
      ConfigurationSource config,
      Supplier<String> scmId,
      ContainerSet containerSet,
      ContainerDownloader downloader,
      VolumeSet volumeSet
  ) {
    super(config, scmId, containerSet, downloader, volumeSet);
  }

  @Override
  public void replicate(ReplicationTask task) {
    long containerID = task.getContainerId();
    if (scmId.get() == null) {
      LOG.error("Replication task is called before first SCM call");
      task.setStatus(Status.FAILED);
    }
    List<DatanodeDetails> sourceDatanodes = task.getSources();

    LOG.info("Starting downloading container {} from {}", containerID,
        sourceDatanodes);

    try {

      long maxContainerSize = (long) config.getStorageSize(
          ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
          ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

      KeyValueContainerData containerData =
          new KeyValueContainerData(containerID,
              ChunkLayOutVersion.FILE_PER_BLOCK, maxContainerSize, "", "");

      //choose a volume
      final HddsVolume volume = volumeChoosingPolicy
          .chooseVolume(volumeSet.getVolumesList(), maxContainerSize);

      //fill the path fields
      containerData.assignToVolume(scmId.get(), volume);

      //download data
      final KeyValueContainerData loadedContainerData =
          downloader
              .getContainerDataFromReplicas(containerData, sourceDatanodes);

      LOG.info("Container {} is download successfully", containerID);
      task.setStatus(Status.DONE);
    } catch (Exception e) {
      LOG.error("Container {} download was unsuccessful.", containerID, e);
      task.setStatus(Status.FAILED);
    }
  }
}
