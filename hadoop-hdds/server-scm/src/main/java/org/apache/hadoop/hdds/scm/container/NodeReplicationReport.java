package org.apache.hadoop.hdds.scm.container;

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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

/**
 * Current statistics about the containers on one specific one.
 */
public class NodeReplicationReport {

  private final DatanodeDetails datanodeInfo;

  private final NodeStatus nodeStatus;
  private final int containers;

  private final int sufficientlyReplicatedContainers;

  public NodeReplicationReport(DatanodeDetails datanodeInfo,
      NodeStatus nodeStatus, int containers,
      int sufficientlyReplicatedContainers) {
    this.datanodeInfo = datanodeInfo;
    this.nodeStatus = nodeStatus;
    this.containers = containers;
    this.sufficientlyReplicatedContainers = sufficientlyReplicatedContainers;
  }

  public DatanodeDetails getDatanodeInfo() {
    return datanodeInfo;
  }

  public int getContainers() {
    return containers;
  }

  public int getSufficientlyReplicatedContainers() {
    return sufficientlyReplicatedContainers;
  }

  public NodeStatus getNodeStatus() {
    return nodeStatus;
  }
}
