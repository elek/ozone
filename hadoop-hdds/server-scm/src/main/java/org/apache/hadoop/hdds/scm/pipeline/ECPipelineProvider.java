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

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements Api for creating stand alone pipelines.
 */
public class ECPipelineProvider extends PipelineProvider<ECReplicationConfig> {

  public ECPipelineProvider(
      NodeManager nodeManager,
      StateManager stateManager
  ) {
    super(nodeManager, stateManager);
  }

  @Override
  public Pipeline create(ECReplicationConfig replicationConfig) throws IOException {
    List<DatanodeDetails> dns = pickNodesNeverUsed(replicationConfig);
    if (dns.size() == 0) {
      String e = String
          .format("Cannot create pipeline of factor %d using %d nodes.",
              replicationConfig.getRequiredNodes(), dns.size());
      throw new InsufficientDatanodesException(e);
    }

    Collections.shuffle(dns);
    final List<DatanodeDetails> nodes = dns.subList(0, 1);
    final Map<DatanodeDetails, Integer> replicationIndexes = new HashMap<>();

    //TODO: this is just a fake variable to check the propagation of the index
    replicationIndexes.put(nodes.get(0), 7);
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setReplicationConfig(replicationConfig)
        .setNodes(nodes)
        .setReplicaIndexes(replicationIndexes)
        .build();
  }

  @Override
  public Pipeline create(
      ECReplicationConfig replicationConfig,
      List<DatanodeDetails> nodes
  ) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(PipelineState.OPEN)
        .setReplicationConfig(replicationConfig)
        .setNodes(nodes)
        .build();
  }

  @Override
  public void close(Pipeline pipeline) throws IOException {

  }

  @Override
  public void shutdown() {
    // Do nothing.
  }
}
