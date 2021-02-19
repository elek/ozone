/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.stream.StreamingClient;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple ContainerDownloaderImplementation to download the missing container
 * from the first available datanode.
 * <p>
 * This is not the most effective implementation as it uses only one source
 * for he container download.
 */
public class SimpleContainerDownloader implements ContainerDownloader {

  private static final Logger LOG =
      LoggerFactory.getLogger(SimpleContainerDownloader.class);

  protected final Path workingDirectory;
  protected  final SecurityConfig securityConfig;
  protected  final X509Certificate caCert;

  public SimpleContainerDownloader(
      ConfigurationSource conf,
      X509Certificate caCert
  ) {

    String workDirString =
        conf.get(OzoneConfigKeys.OZONE_CONTAINER_COPY_WORKDIR);

    if (workDirString == null) {
      workingDirectory = Paths.get(System.getProperty("java.io.tmpdir"))
          .resolve("container-copy");
    } else {
      workingDirectory = Paths.get(workDirString);
    }
    securityConfig = new SecurityConfig(conf);
    this.caCert = caCert;
  }

  @Override
  public KeyValueContainerData getContainerDataFromReplicas(
      KeyValueContainerData containerData,
      List<DatanodeDetails> sourceDatanodes
  ) {

    final List<DatanodeDetails> shuffledDatanodes =
        shuffleDatanodes(sourceDatanodes);

    for (DatanodeDetails datanode : shuffledDatanodes) {
      try {
        return downloadContainer(containerData, datanode);
      } catch (Exception ex) {
        LOG.error(String.format(
            "Container %s download from datanode %s was unsuccessful. "
                + "Trying the next datanode", containerData.getContainerID(),
            datanode), ex);
      }
    }
    throw new RuntimeException(
        "Couldn't download container from any of the datanodes " + containerData
            .getContainerID());

  }

  //There is a chance for the download is successful but import is failed,
  //due to data corruption. We need a random selected datanode to have a
  //chance to succeed next time.
  @NotNull
  protected List<DatanodeDetails> shuffleDatanodes(
      List<DatanodeDetails> sourceDatanodes
  ) {

    final ArrayList<DatanodeDetails> shuffledDatanodes =
        new ArrayList<>(sourceDatanodes);

    Collections.shuffle(shuffledDatanodes);

    return shuffledDatanodes;
  }

  @VisibleForTesting
  protected KeyValueContainerData downloadContainer(
      KeyValueContainerData preCreated,
      DatanodeDetails datanode
  ) throws IOException {
    //    CompletableFuture<Path> result;
    //
    try {
      StreamingClient client =
          new StreamingClient(datanode.getIpAddress(), datanode.getPort(
              Name.REPLICATION).getValue(),
              new ContainerStreamingDestination(preCreated));
      final Channel channel = client.connect();
      channel.writeAndFlush(preCreated.getContainerID() + "\n");
      channel.closeFuture().sync().addListener(f -> {
        LOG.info("Container " + preCreated.getContainerID()
            + " is downloaded succesfully");
      });
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }


        //parse descriptor
        //now, we have extracted the container descriptor from the previous
        //datanode. We can load it and upload it with the current data
        // (original metadata + current filepath fields)
        KeyValueContainerData replicated =
            (KeyValueContainerData) ContainerDataYaml
                .readContainer(descriptor);

        KeyValueContainerData updated = new KeyValueContainerData(
            replicated.getContainerID(),
            replicated.getLayOutVersion(),
            replicated.getMaxSize(),
            replicated.getOriginPipelineId(),
            replicated.getOriginNodeId());

        //inherited from the replicated
        updated
            .setState(replicated.getState());
        updated
            .setContainerDBType(replicated.getContainerDBType());
        updated
            .updateBlockCommitSequenceId(replicated
           .getBlockCommitSequenceId());
        updated
            .setSchemaVersion(replicated.getSchemaVersion());

        //inherited from the pre-created seed container
        updated.setMetadataPath(preCreated.getMetadataPath());
        updated.setDbFile(preCreated.getDbFile());
        updated.setChunksPath(preCreated.getChunksPath());
        updated.setVolume(preCreated.getVolume());

        return updated;
    return preCreated;
  }

  @Override
  public void close() {
    // noop
  }
}
