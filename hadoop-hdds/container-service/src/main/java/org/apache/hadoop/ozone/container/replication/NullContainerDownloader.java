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
import java.io.OutputStream;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import com.google.common.annotations.VisibleForTesting;

/**
 * Downloads bytes and drop them.
 *
 * Usable only for testing.
 */
public class NullContainerDownloader extends SimpleContainerDownloader {

  public NullContainerDownloader(
      ConfigurationSource conf,
      X509Certificate caCert
  ) {
    super(conf, caCert);
  }

  @VisibleForTesting
  protected KeyValueContainerData downloadContainer(
      KeyValueContainerData preCreated,
      DatanodeDetails datanode
  ) throws IOException {
    CompletableFuture<Path> result;
    GrpcReplicationClient grpcReplicationClient =
        new GrpcReplicationClient(datanode.getIpAddress(),
            datanode.getPort(Name.REPLICATION).getValue(),
            workingDirectory, securityConfig, caCert);

    OutputStream nullOutputStream = new OutputStream() {
      @Override
      public void write(int i) throws IOException {
        //
      }
    };
    grpcReplicationClient.download(preCreated, nullOutputStream);
    return preCreated;
  }

  @Override
  public void close() {
    // noop
  }
}
