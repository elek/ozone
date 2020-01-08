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
package org.apache.hadoop.hdds.scm.container.states;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSING;
import org.junit.Before;
import org.junit.Test;
import java.util.*;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.OPEN;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State
    .DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State
    .MAINTENANCE;
import static org.junit.Assert.assertFalse;

/**
 * Class used to test the ContainerReplicaCount class.
 */
public class TestContainerReplicaCount {

  @Before
  public void setup() {
  }

  private Map<UUID, NodeStatus> nodeStatusMap(List<DatanodeInfo> datanodes) {
    return null;
  }

  private List<DatanodeInfo> createDatanodes(
      HddsProtos.NodeOperationalState... states) {
    return Arrays.stream(states)
        .map(state -> {
          DatanodeDetails dn = TestUtils.randomDatanodeDetails();
          return new DatanodeInfo(dn, new NodeStatus(state, NodeState.HEALTHY));
        })
        .collect(Collectors.toList());
  }

  @Test
  public void testThreeHealthyReplica() {
    List<DatanodeInfo> datanodes =
        createDatanodes(NodeOperationalState.IN_SERVICE,
            NodeOperationalState.IN_SERVICE, NodeOperationalState.IN_SERVICE);

    Set<ContainerReplica> replica =
        addContainers(datanodes, CLOSED, CLOSED, CLOSED);

    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, nodeStatusMap(datanodes), replica,
            0, 0, 3,
            2);
    validate(rcnt, true, 0, false);
  }

  @Test
  public void testTwoHealthyReplica() {
    List<DatanodeInfo> datanodes =
        createDatanodes(NodeOperationalState.IN_SERVICE,
            NodeOperationalState.IN_SERVICE);
    Set<ContainerReplica> replica = addContainers(datanodes, CLOSED, CLOSED);
    ContainerInfo container = createContainer(HddsProtos.LifeCycleState.CLOSED);
    ContainerReplicaCount rcnt =
        new ContainerReplicaCount(container, nodeStatusMap(datanodes),
            replica, 0, 0, 3, 2);
    validate(rcnt, false, 1, false);
  }

  //  @Test
  //  public void testOneHealthyReplica() {
  //    Set<ContainerReplica> replica = createDatanodeWithContainers(CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    validate(rcnt, false, 2, false);
  //  }
  //
  //  @Test
  //  public void testTwoHealthyAndInflightAdd() {
  //
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 1, 0, 3, 2);
  //    validate(rcnt, false, 0, false);
  //  }
  //
  //  @Test
  //  /**
  //   * This does not schedule a container to be removed, as the inFlight
  //   add may
  //   * fail and then the delete would make things under-replicated. Once
  //   the add
  //   * completes there will be 4 healthy and it will get taken care of then.
  //   */
  //  public void testThreeHealthyAndInflightAdd() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 1, 0, 3, 2);
  //    validate(rcnt, true, 0, false);
  //  }
  //
  //  @Test
  //  /**
  //   * As the inflight delete may fail, but as it will make the the container
  //   * under replicated, we go ahead and schedule another replica to be added.
  //   */
  //  public void testThreeHealthyAndInflightDelete() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 1, 3, 2);
  //    validate(rcnt, false, 1, false);
  //  }
  //
  //  @Test
  //  /**
  //   * This is NOT sufficiently replicated as the inflight add may fail and
  //   the
  //   * inflight del could succeed, leaving only 2 healthy replicas.
  //   */
  //  public void testThreeHealthyAndInflightAddAndInFlightDelete() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 1, 1, 3, 2);
  //    validate(rcnt, false, 0, false);
  //  }
  //
  //  @Test
  //  public void testFourHealthyReplicas() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, CLOSED, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    validate(rcnt, true, -1, true);
  //  }
  //
  //  @Test
  //  public void testFourHealthyReplicasAndInFlightDelete() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, CLOSED, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 1, 3, 2);
  //    validate(rcnt, true, 0, false);
  //  }
  //
  //  @Test
  //  public void testFourHealthyReplicasAndTwoInFlightDelete() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, CLOSED, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 2, 3, 2);
  //    validate(rcnt, false, 1, false);
  //  }
  //
  //  @Test
  //  public void testOneHealthyReplicaRepFactorOne() {
  //    Set<ContainerReplica> replica = createDatanodeWithContainers(CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 1, 2);
  //    validate(rcnt, true, 0, false);
  //  }
  //
  //  @Test
  //  public void testOneHealthyReplicaRepFactorOneInFlightDelete() {
  //    Set<ContainerReplica> replica = createDatanodeWithContainers(CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 1, 1, 2);
  //    validate(rcnt, false, 1, false);
  //  }
  //
  //  @Test
  //  public void testTwoHealthyReplicaTwoInflightAdd() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 2, 0, 3, 2);
  //    validate(rcnt, false, 0, false);
  //  }
  //
  //  /**
  //   * From here consider decommission replicas.
  //   */
  //
  //  @Test
  //  public void testThreeHealthyAndTwoDecommission() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, CLOSED,
  //        DECOMMISSIONED, DECOMMISSIONED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    validate(rcnt, true, 0, false);
  //  }
  //
  //  @Test
  //  public void testOneDecommissionedReplica() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, DECOMMISSIONED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    validate(rcnt, false, 1, false);
  //  }
  //
  //  @Test
  //  public void testTwoHealthyOneDecommissionedneInFlightAdd() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, DECOMMISSIONED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 1, 0, 3, 2);
  //    validate(rcnt, false, 0, false);
  //  }
  //
  //  @Test
  //  public void testAllDecommissioned() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(DECOMMISSIONED, DECOMMISSIONED,
  //            DECOMMISSIONED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    validate(rcnt, false, 3, false);
  //  }
  //
  //  @Test
  //  public void testAllDecommissionedRepFactorOne() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(DECOMMISSIONED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 1, 2);
  //    validate(rcnt, false, 1, false);
  //
  //  }
  //
  //  @Test
  //  public void testAllDecommissionedRepFactorOneInFlightAdd() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(DECOMMISSIONED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 1, 0, 1, 2);
  //    validate(rcnt, false, 0, false);
  //  }
  //
  //  @Test
  //  public void testOneHealthyOneDecommissioningRepFactorOne() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(DECOMMISSIONED, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 1, 2);
  //    validate(rcnt, true, 0, false);
  //  }
  //
  //  /**
  //   * Maintenance tests from here.
  //   */
  //
  //  @Test
  //  public void testOneHealthyTwoMaintenanceMinRepOfTwo() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, MAINTENANCE, MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    validate(rcnt, false, 1, false);
  //  }
  //
  //  @Test
  //  public void testOneHealthyThreeMaintenanceMinRepOfTwo() {
  //    Set<ContainerReplica> replica = createDatanodeWithContainers(CLOSED,
  //        MAINTENANCE, MAINTENANCE, MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    validate(rcnt, false, 1, false);
  //  }
  //
  //  @Test
  //  public void testOneHealthyTwoMaintenanceMinRepOfOne() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, MAINTENANCE, MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 1);
  //    validate(rcnt, true, 0, false);
  //  }
  //
  //  @Test
  //  public void testOneHealthyThreeMaintenanceMinRepOfTwoInFlightAdd() {
  //    Set<ContainerReplica> replica = createDatanodeWithContainers(CLOSED,
  //        MAINTENANCE, MAINTENANCE, MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 1, 0, 3, 2);
  //    validate(rcnt, false, 0, false);
  //  }
  //
  //  @Test
  //  public void testAllMaintenance() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(MAINTENANCE, MAINTENANCE, MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    validate(rcnt, false, 2, false);
  //  }
  //
  //  @Test
  //  /**
  //   * As we have exactly 3 healthy, but then an excess of maintenance copies
  //   * we ignore the over-replication caused by the maintenance copies
  //   until they
  //   * come back online, and then deal with them.
  //   */
  //  public void testThreeHealthyTwoInMaintenance() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, CLOSED,
  //        MAINTENANCE, MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    validate(rcnt, true, 0, false);
  //  }
  //
  //  @Test
  //  /**
  //   * This is somewhat similar to testThreeHealthyTwoInMaintenance()
  //   except now
  //   * one of the maintenance copies has become healthy and we will need to
  //   remove
  //   * the over-replicated healthy container.
  //   */
  //  public void testFourHealthyOneInMaintenance() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, CLOSED, CLOSED,
  //            MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    validate(rcnt, true, -1, true);
  //  }
  //
  //  @Test
  //  public void testOneMaintenanceMinRepOfTwoRepFactorOne() {
  //    Set<ContainerReplica> replica = createDatanodeWithContainers
  //    (MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 1, 2);
  //    validate(rcnt, false, 1, false);
  //  }
  //
  //  @Test
  //  public void testOneMaintenanceMinRepOfTwoRepFactorOneInFlightAdd() {
  //    Set<ContainerReplica> replica = createDatanodeWithContainers
  //    (MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 1, 0, 1, 2);
  //    validate(rcnt, false, 0, false);
  //  }
  //
  //  @Test
  //  public void testOneHealthyOneMaintenanceRepFactorOne() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(MAINTENANCE, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 1, 2);
  //    validate(rcnt, true, 0, false);
  //  }
  //
  //  @Test
  //  public void testTwoDecomTwoMaintenanceOneInflightAdd() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(DECOMMISSIONED, DECOMMISSIONED,
  //            MAINTENANCE, MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 1, 0, 3, 2);
  //    validate(rcnt, false, 1, false);
  //  }
  //
  //  @Test
  //  public void testHealthyContainerIsHealthy() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, CLOSED);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    assertTrue(rcnt.isHealthy());
  //  }
  //
  //  @Test
  //  public void testIsHealthyWithDifferentReplicaStateNotHealthy() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, OPEN);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    assertFalse(rcnt.isHealthy());
  //  }
  //
  //  @Test
  //  public void testIsHealthyWithMaintReplicaIsHealthy() {
  //    Set<ContainerReplica> replica =
  //        createDatanodeWithContainers(CLOSED, CLOSED, MAINTENANCE,
  //        MAINTENANCE);
  //    ContainerInfo container = createContainer(HddsProtos.LifeCycleState
  //    .CLOSED);
  //    ContainerReplicaCount rcnt =
  //        new ContainerReplicaCount(container, replica, 0, 0, 3, 2);
  //    assertTrue(rcnt.isHealthy());
  //  }

  private void validate(ContainerReplicaCount rcnt,
      boolean sufficientlyReplicated, int replicaDelta,
      boolean overReplicated) {
    assertEquals(sufficientlyReplicated, rcnt.isSufficientlyReplicated());
    assertEquals(overReplicated, rcnt.isOverReplicated());
    assertEquals(replicaDelta, rcnt.additionalReplicaNeeded());
  }

  private Set<ContainerReplica> addContainers(
      List<DatanodeInfo> datanodeInfo,
      ContainerReplicaProto.State... states) {
    if (datanodeInfo.size() != states.length) {
      throw new IllegalArgumentException(
          "Use same number of containers and datanodes");
    }
    Set<ContainerReplica> replica = new HashSet<>();

    for (int i = 0; i < states.length; i++) {
      replica.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(new ContainerID(i))
          .setContainerState(states[i])
          .setDatanodeDetails(datanodeInfo.get(i))
          .setOriginNodeId(datanodeInfo.get(i).getUuid())
          .setSequenceId(1)
          .build());
    }
    return replica;
  }

  private ContainerInfo createContainer(HddsProtos.LifeCycleState state) {
    return new ContainerInfo.Builder()
        .setContainerID(new ContainerID(1).getId())
        .setState(state)
        .build();
  }
}
