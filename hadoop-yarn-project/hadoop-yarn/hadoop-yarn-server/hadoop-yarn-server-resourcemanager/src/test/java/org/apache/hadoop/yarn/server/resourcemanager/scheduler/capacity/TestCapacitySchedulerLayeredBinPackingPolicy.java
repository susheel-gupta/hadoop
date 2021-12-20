/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.waitforNMRegistered;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.LayeredNodeUsageBinPackingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSorter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(Enclosed.class)
public class TestCapacitySchedulerLayeredBinPackingPolicy {

  /* This testclass validates Multi Node Placement Feature.
   */
  public static class TestBinPackingAlgorithm {

    private static final Log LOG = LogFactory
        .getLog(TestBinPackingAlgorithm.class);
    private CapacitySchedulerConfiguration conf;
    private static final String POLICY_CLASS_NAME =
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement."
        + "LayeredNodeUsageBinPackingPolicy";

    @Before
    public void setUp() {
      CapacitySchedulerConfiguration config =
          new CapacitySchedulerConfiguration();
      config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          DominantResourceCalculator.class.getName());
      conf = new CapacitySchedulerConfiguration(config);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
          "resource-based");
      conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
          "resource-based");
      String policyName =
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
          + ".resource-based" + ".class";
      conf.set(policyName, POLICY_CLASS_NAME);
      conf.setBoolean(CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED,
          true);
      // Set this to avoid the AM pending issue
      conf.set(CapacitySchedulerConfiguration
          .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, "1");
      conf.setInt("yarn.scheduler.minimum-allocation-mb", 512);
      conf.setInt("yarn.scheduler.minimum-allocation-vcores", 1);
      conf.setInt("yarn.scheduler.maximum-allocation-mb", 102400);

      // Configure two queues to test Preemption
      conf.set("yarn.scheduler.capacity.root.queues", "A, default");
      conf.set("yarn.scheduler.capacity.root.A.capacity", "50");
      conf.set("yarn.scheduler.capacity.root.default.capacity", "50");
      conf.set("yarn.scheduler.capacity.root.A.maximum-capacity", "100");
      conf.set("yarn.scheduler.capacity.root.default.maximum-capacity", "100");
      conf.set("yarn.scheduler.capacity.root.A.user-limit-factor", "10");
      conf.set("yarn.scheduler.capacity.root.default.user-limit-factor", "10");

      // Configure Preemption
      conf.setLong(
          CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL, 1000);
      conf.setLong(CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
          300);
      conf.setFloat(CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
          1.0f);
      conf.setFloat(
          CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
          1.0f);
      conf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
          ProportionalCapacityPreemptionPolicy.class.getCanonicalName());
      conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
      conf.set("yarn.scheduler.capacity.schedule-asynchronously.enable", "true");
    }

    /**
     * Test the scorer output given a usage number.
     */
    @Test(timeout=30000)
    public void testLayeredResourceUsageScorer() {
      LayeredNodeUsageBinPackingPolicy policy = new LayeredNodeUsageBinPackingPolicy(true);
      // threshold 0.6
      float threshold = (float) 0.6;
      float currentUsage = (float) 0.2;
      float normalNodeScore = policy.scorer(currentUsage, threshold);
      // node with resource usage 0.2
      assertEquals(2000 + 3 * currentUsage * 100, normalNodeScore, 0);
      // node with resource usage 0.5
      currentUsage = (float) 0.5;
      assertEquals(2000 + 3 * currentUsage * 100,
          policy.scorer(currentUsage, threshold), 0);
      // node with resource usage 0.6
      currentUsage = (float) 0.6;
      assertEquals(1000 - 3 * currentUsage * 100,
          policy.scorer(currentUsage, threshold), 0);
      // node with resource usage 0.7
      currentUsage = (float) 0.7;
      float busyNodeScore = policy.scorer(currentUsage, threshold);
      assertEquals(1000 - 3 * currentUsage * 100,
          policy.scorer(currentUsage, threshold), 0);
      // node with resource usage 0.9
      currentUsage = (float) 0.9;
      float busyNodeScore2 = policy.scorer(currentUsage, threshold);
      assertEquals(1000 - 3 * currentUsage * 100,
          policy.scorer(currentUsage, threshold), 0);
      // node with resource usage 0, the score shouldn't exceed 100
      currentUsage = (float) 0.0;
      float freeNodeScore = policy.scorer(currentUsage, threshold);

      assertTrue("Node usage 0 shoudn't have a score higher than 100. Actual:"
          + freeNodeScore, freeNodeScore < 100.0);
      assertTrue(
          "High usage busy node score should be smaller to avoid hot spot",
          busyNodeScore2 < busyNodeScore);
      assertTrue("Busy node score should be smaller than normal node",
          busyNodeScore < normalNodeScore);
      assertTrue("Busy node score should be larger than free node",
          busyNodeScore > freeNodeScore);
    }

    /**
     * The container requests will generate nodes with resource usage
     * 0.6, 0.5, 0.7 gradually. Then it will verify the nodes order.
     */
    @Test(timeout=30000)
    public void testMultiNodeSorterForLayeredBinPacking() throws Exception {
      MockRM rm = new MockRM(conf);
      rm.start();
      MockNM[] nms = new MockNM[4];
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 100 * GB, 100);
      MockNM nm2 = rm.registerNode("127.0.0.2:1235", 100 * GB, 100);
      MockNM nm3 = rm.registerNode("127.0.0.3:1236", 100 * GB, 100);
      MockNM nm4 = rm.registerNode("127.0.0.4:1237", 100 * GB, 100);

      nms[0] = nm4;
      nms[1] = nm3;
      nms[2] = nm2;
      nms[3] = nm1;

      ResourceScheduler scheduler = rm.getRMContext().getScheduler();
      waitforNMRegistered(scheduler, 4, 5);

      MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
          .getMultiNodeSortingManager();
      MultiNodeSorter<SchedulerNode> sorter = mns
          .getMultiNodePolicy(POLICY_CLASS_NAME);
      sorter.reSortClusterNodes();

      Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
          .getNodesPerPartition("");
      assertEquals(4, nodes.size());

      RMApp app1 = rm.submitApp(2 * GB, "app-1", "user1", null, "default");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);

      MockNM firstChosenNode = null;
      int firstChosenNodeIndex = 0;
      for (int i = 0; i < nms.length; i++) {
        SchedulerNodeReport reportNM =
            rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
        if (reportNM.getUsedResource().getMemorySize() == 2 * GB) {
          firstChosenNode = nms[i];
          firstChosenNodeIndex = i;
          break;
        }
      }

      // check node report
      SchedulerNodeReport reportNM = rm.getResourceScheduler()
          .getNodeReport(firstChosenNode.getNodeId());
      assertEquals(2 * GB, reportNM.getUsedResource().getMemorySize());
      assertEquals((100 - 2) * GB,
          reportNM.getAvailableResource().getMemorySize());

      // Ideally thread will invoke this, but thread operates every 1sec.
      // Hence forcefully recompute nodes.
      sorter.reSortClusterNodes();

      // Another app will be scheduled in the first chosen node
      RMApp app2 = rm.submitApp(1 * GB, "app-2", "user2", null, "default");
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nms);
      am2.allocateAndWaitForContainers("*", 1, 5 * GB, nm1);

      // check node report
      reportNM =
          rm.getResourceScheduler().getNodeReport(firstChosenNode.getNodeId());
      assertEquals(8 * GB, reportNM.getUsedResource().getMemorySize());
      assertEquals((100 - 8) * GB,
          reportNM.getAvailableResource().getMemorySize());

      // am request containers. Now totally 60GB, the first chosen node will fall
      // into busy layer
      am1.allocateAndWaitForContainers("*", 1, 52 * GB, nm1);

      // the above resource request should be allocated to first chosen node
      reportNM =
          rm.getResourceScheduler().getNodeReport(firstChosenNode.getNodeId());
      assertEquals(60 * GB, reportNM.getUsedResource().getMemorySize());
      assertEquals((100 - 60) * GB,
          reportNM.getAvailableResource().getMemorySize());

      // This container request cannot be served by the first chosen node.
      // It should pick a random node as second chosen node
      am1.allocateAndWaitForContainers("*", 1, 50 * GB, nm1);

      // first chosen node doesn't change usage
      reportNM =
          rm.getResourceScheduler().getNodeReport(firstChosenNode.getNodeId());
      assertEquals(60 * GB, reportNM.getUsedResource().getMemorySize());
      assertEquals((100 - 60) * GB,
          reportNM.getAvailableResource().getMemorySize());

      // This container request cannot be served by first and second. Pick another
      // random one
      am1.allocateAndWaitForContainers("*", 1, 70 * GB, nm1);

      // Get the second chosen node
      MockNM secondChosenNode = null;
      int secondChosenNodeIndex = 0;
      MockNM thirdChosenNode = null;
      int thirdChosenNodeIndex = 0;
      for (int i = 0; i < nms.length; i++) {
        reportNM =
            rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
        if (reportNM.getUsedResource().getMemorySize() == 50 * GB) {
          secondChosenNode = nms[i];
          secondChosenNodeIndex = i;
        }
        if (reportNM.getUsedResource().getMemorySize() == 70 * GB) {
          thirdChosenNode = nms[i];
          thirdChosenNodeIndex = i;
        }
      }

      SchedulerNodeReport reportSecondNM =
          rm.getResourceScheduler().getNodeReport(secondChosenNode.getNodeId());
      assertEquals(50 * GB, reportSecondNM.getUsedResource().getMemorySize());
      assertEquals((100 - 50) * GB,
          reportSecondNM.getAvailableResource().getMemorySize());

      SchedulerNodeReport reportThirdNM =
          rm.getResourceScheduler().getNodeReport(thirdChosenNode.getNodeId());
      assertEquals(70 * GB, reportThirdNM.getUsedResource().getMemorySize());
      assertEquals((100 - 70) * GB,
          reportThirdNM.getAvailableResource().getMemorySize());

      for (int i = 0; i < nms.length; i++) {
        if (i == firstChosenNodeIndex) {
          continue;
        }
        if (i == secondChosenNodeIndex) {
          continue;
        }
        if (i == thirdChosenNodeIndex) {
          continue;
        }
        reportNM =
            rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
        assertEquals(0 * GB, reportNM.getUsedResource().getMemorySize());
        assertEquals((100 - 0) * GB,
            reportNM.getAvailableResource().getMemorySize());
      }

      // Ideally thread will invoke this, but thread operates every 1sec.
      // Hence forcefully recompute nodes.
      sorter.reSortClusterNodes();

      nodes = sorter.getMultiNodeLookupPolicy()
          .getNodesPerPartition("");
      List<NodeId> currentNodes = new ArrayList<>();
      // second one usage is 0.5, first one usage is 0.6, third usage is 0.7
      currentNodes.add(nms[secondChosenNodeIndex].getNodeId());
      currentNodes.add(nms[firstChosenNodeIndex].getNodeId());
      currentNodes.add(nms[thirdChosenNodeIndex].getNodeId());
      Iterator<SchedulerNode> it = nodes.iterator();
      SchedulerNode current;
      int i = 0;
      while (it.hasNext()) {
        current = it.next();
        assertEquals(current.getNodeID(), currentNodes.get(i++));
        if (i == 2) {
          break;
        }
      }
      rm.stop();
    }

    /**
     * This is to test reservation won't block allocating pending container when
     * there's available resource in the other nodes
     * In this case, when node1 has no enought resource to use,
     * but node2 has available resource,
     * There should be no reservation created which make containers pending there.
     */
    @Test(timeout=30000)
    public void testSkipReservationWhenPossible() throws Exception {
      MockRM rm = new MockRM(conf);
      rm.start();
      MockNM[] nms = new MockNM[2];
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 13 * GB, 100);
      MockNM nm2 = rm.registerNode("127.0.0.2:1235", 13 * GB, 100);

      nms[0] = nm2;
      nms[1] = nm1;

      ResourceScheduler scheduler = rm.getRMContext().getScheduler();
      waitforNMRegistered(scheduler, 2, 5);

      MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
          .getMultiNodeSortingManager();
      MultiNodeSorter<SchedulerNode> sorter = mns
          .getMultiNodePolicy(POLICY_CLASS_NAME);
      sorter.reSortClusterNodes();

      Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
          .getNodesPerPartition("");
      assertEquals(2, nodes.size());

      RMApp app1 = rm.submitApp(2 * GB, "app-1", "user1", null, "default");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);

      // Get the first chosen node
      MockNM firstChosenNode = null;
      int firstChosenNodeIndex = 0;
      MockNM secondChosenNode = null;
      int secondChosenNodeIndex = 0;
      for (int i = 0; i < nms.length; i++) {
        SchedulerNodeReport reportNM =
            rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
        if (reportNM.getUsedResource().getMemorySize() == 2 * GB) {
          firstChosenNode = nms[i];
          firstChosenNodeIndex = i;
        } else {
          secondChosenNode = nms[i];
          secondChosenNodeIndex = i;
        }
      }
      // check node report
      SchedulerNodeReport reportNM = rm.getResourceScheduler().
          getNodeReport(firstChosenNode.getNodeId());
      assertEquals(2 * GB, reportNM.getUsedResource().getMemorySize());
      assertEquals((13 - 2) * GB,
          reportNM.getAvailableResource().getMemorySize());

      // Ideally thread will invoke this, but thread operates every 1sec.
      // Hence forcefully recompute nodes.
      sorter.reSortClusterNodes();

      RMApp app2 = rm.submitApp(1 * GB, "app-2", "user2", null, "default");
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nms);
      am2.allocateAndWaitForContainers("*", 1, 5 * GB, nm2);

      // check node report
      SchedulerNodeReport reportNm2 =
          rm.getResourceScheduler().getNodeReport(secondChosenNode.getNodeId());
      assertEquals(0 * GB, reportNm2.getUsedResource().getMemorySize());
      assertEquals(13 * GB,
          reportNm2.getAvailableResource().getMemorySize());

      // check node report
      reportNM = rm.getResourceScheduler().
          getNodeReport(firstChosenNode.getNodeId());
      assertEquals(8 * GB, reportNM.getUsedResource().getMemorySize());
      assertEquals((13 - 8) * GB,
          reportNM.getAvailableResource().getMemorySize());

      // am request containers, this request should be served by second node
      // due to first one has no headroom
      am2.allocateAndWaitForContainers("*", 1, 6 * GB, nm2);

      // check node report
      reportNM =
          rm.getResourceScheduler().getNodeReport(firstChosenNode.getNodeId());
      assertEquals(8 * GB, reportNM.getUsedResource().getMemorySize());
      assertEquals((13 - 8) * GB,
          reportNM.getAvailableResource().getMemorySize());

      reportNm2 =
          rm.getResourceScheduler().getNodeReport(secondChosenNode.getNodeId());
      assertEquals(6 * GB, reportNm2.getUsedResource().getMemorySize());
      assertEquals((13 - 6) * GB,
              reportNm2.getAvailableResource().getMemorySize());

      // Ideally thread will invoke this, but thread operates every 1sec.
      // Hence forcefully recompute nodes.
      sorter.reSortClusterNodes();

      nodes = sorter.getMultiNodeLookupPolicy()
          .getNodesPerPartition("");
      List<NodeId> currentNodes = new ArrayList<>();
      // first chosen node usage is 8/13 (busy layer)
      // second chosen node usage is 6/13 (normal layer)
      currentNodes.add(secondChosenNode.getNodeId());
      currentNodes.add(firstChosenNode.getNodeId());
      Iterator<SchedulerNode> it = nodes.iterator();
      SchedulerNode current;
      int i = 0;
      while (it.hasNext()) {
        current = it.next();
        assertEquals(current.getNodeID(), currentNodes.get(i++));
      }
      rm.stop();
    }

    /**
     * This is to test that big containers should be reserved when there's
     * no headroom in the cluster. When there's headroom,
     * it will allocate this container from reservation
     */
    @Test(timeout=30000)
    public void testReserveContainerAndThenAllocated() throws Exception {
      MockRM rm = new MockRM(conf);
      rm.start();
      MockNM[] nms = new MockNM[2];
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 15 * GB, 3);
      MockNM nm2 = rm.registerNode("127.0.0.2:1235", 5 * GB, 3);

      nms[0] = nm2;
      nms[1] = nm1;

      ResourceScheduler scheduler = rm.getRMContext().getScheduler();
      waitforNMRegistered(scheduler, 2, 5);

      MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
          .getMultiNodeSortingManager();
      MultiNodeSorter<SchedulerNode> sorter = mns
          .getMultiNodePolicy(POLICY_CLASS_NAME);
      sorter.reSortClusterNodes();

      Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
          .getNodesPerPartition("");
      assertEquals(2, nodes.size());

      RMApp app1 = rm.submitApp(10 * GB, "app-1", "user1", null, "default");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);

      // check node report
      SchedulerNodeReport reportNm1 =
          rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
      assertEquals(10 * GB, reportNm1.getUsedResource().getMemorySize());
      assertEquals(5 * GB,
          reportNm1.getAvailableResource().getMemorySize());

      // Ideally thread will invoke this, but thread operates every 1sec.
      // Hence forcefully recompute nodes.
      sorter.reSortClusterNodes();

      // Reserve a Container for app2
      final AtomicBoolean result = new AtomicBoolean(false);
      RMApp app2 = rm.submitApp(10 * GB, "app-2", "user1", null, "default");
      Thread t = new Thread() {
        public void run() {
          try {
            MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nms);
            result.set(true);
          } catch (Exception e) {
            Assert.fail("Failed to Allocate Reserved Container");
          }
        }
      };
      t.start();
      Thread.sleep(1000);

      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      LeafQueue leafQueue = (LeafQueue) cs.getQueue("default");

      FiCaSchedulerApp schedulerApp = getFiCaSchedulerApp(leafQueue,
          app2.getApplicationId());
      assertEquals("App2 failed to get reserved container", 1,
          schedulerApp.getReservedContainers().size());

      // Reservation will happen on the last node when none of them can fit
      assertNotNull("Failed to reserve container in Node2",
          cs.getNode(nm2.getNodeId()).getReservedContainer());

      // Kill app1
      rm.killApp(app1.getApplicationId());
      heartbeat(rm, nm1);

      // Check if Reserved AM of app2 gets allocated
      while (!result.get()) {
        Thread.sleep(10);
      }
      reportNm1 =
          rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
      assertEquals(10 * GB, reportNm1.getUsedResource().getMemorySize());
      assertEquals(5 * GB,
          reportNm1.getAvailableResource().getMemorySize());
      SchedulerNodeReport reportNm2 =
          rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
      assertEquals(0 * GB, reportNm2.getUsedResource().getMemorySize());
      assertNull("Failed to unreserve container in Node2",
          cs.getNode(nm2.getNodeId()).getReservedContainer());
      schedulerApp = getFiCaSchedulerApp(leafQueue, app2.getApplicationId());
      assertEquals("Failed to unreserve container in App2", 0,
          schedulerApp.getReservedContainers().size());
      rm.stop();
    }

    /**
     * This verifies if an appA AM container reserved on node A gets allocated
     * when node B has available space. The CS#allocateOrReserveNewContainers
     * allocates a new container to appA, CS#allocateFromReservedContainer will
     * release the reserved container from node A.
     */
    @Test(timeout=30000)
    public void testAllocateReservedAMContainerFromOtherNode() throws Exception {
      MockRM rm = new MockRM(conf);
      rm.start();
      MockNM[] nms = new MockNM[4];
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 25 * GB, 3);
      nms[0] = nm1;
      MockNM nm2 = rm.registerNode("127.0.0.2:1234", 15 * GB, 3);
      nms[1] = nm2;

      MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
          .getMultiNodeSortingManager();
      MultiNodeSorter<SchedulerNode> sorter = mns
          .getMultiNodePolicy(POLICY_CLASS_NAME);

      sorter.reSortClusterNodes();

      // launch an app to queue, AM container will be launched in nm1
      RMApp app1 = rm.submitApp(20 * GB, "app-1", "user1", null, "default");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      // launch an app to queue, AM container will be launched in nm2
      RMApp app2 = rm.submitApp(10 * GB, "app-2", "user1", null, "default");
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

      sorter.reSortClusterNodes();

      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
      RMNode rmNode2 = rm.getRMContext().getRMNodes().get(nm2.getNodeId());

      // Reserve a Container for app3
      final AtomicBoolean result = new AtomicBoolean(false);
      Thread t = new Thread() {
        public void run() {
          try {
            RMApp app3 = rm.submitApp(10 * GB, "app-3", "user1", null, "default");
            MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm2);
            result.set(true);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };
      t.start();
      Thread.sleep(1000);

      // Free the Space on other node where Reservation has not happened
      if (cs.getNode(rmNode1.getNodeID()).getReservedContainer() != null) {
        rm.killApp(app2.getApplicationId());
        cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
      } else if (cs.getNode(rmNode2.getNodeID()).getReservedContainer() != null) {
        rm.killApp(app1.getApplicationId());
        cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      } else {
        Assert.fail("Failed to reserve container");
      }

      // Check if Reserved AM of app3 gets allocated in
      // node where space available
      while (!result.get()) {
        Thread.sleep(10);
      }
      rm.stop();
    }


    /**
     * This verifies if an appA reserved containers are allocated after
     * Preemption
     **/
    @Test(timeout=30000)
    public void testAllocateReservedContainerAfterPreemption() throws Exception {
      MockRM rm = new MockRM(conf);
      rm.start();
      MockNM[] nms = new MockNM[4];
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 100 * GB, 100);
      nms[0] = nm1;
      MockNM nm2 = rm.registerNode("127.0.0.2:1234", 100 * GB, 100);
      nms[1] = nm2;
      MockNM nm3 = rm.registerNode("127.0.0.3:1234", 100 * GB, 100);
      nms[2] = nm3;
      MockNM nm4 = rm.registerNode("127.0.0.4:1234", 100 * GB, 100);
      nms[3] = nm4;

      MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
          .getMultiNodeSortingManager();
      MultiNodeSorter<SchedulerNode> sorter = mns
          .getMultiNodePolicy(POLICY_CLASS_NAME);

      sorter.reSortClusterNodes();

      // launch an app to queue, AM container will be launched in nm1
      RMApp app1 = rm.submitApp(100 * GB, "app-1", "user1", null, "default");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      // Allocate three more containers so that cluster is full
      am1.allocateAndWaitForContainers("127.0.0.2", 1, 100 * GB, nm2);
      am1.allocateAndWaitForContainers("127.0.0.3", 1, 100 * GB, nm3);
      am1.allocateAndWaitForContainers("127.0.0.4", 1, 100 * GB, nm4);

      sorter.reSortClusterNodes();

      // Launch an app to queue, AM container will be reserved
      // Validate if Allocation of Reserved container happens
      // after Preemption
      RMApp app2 = rm.submitApp(100 * GB, "app-2", "user2", null, "A");
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nms);
      am2.allocateAndWaitForContainers("127.0.0.3", 1, 100 * GB, nm3);

      rm.stop();
    }
  }

  /**
   * Single node look-up in the global scheduling.
   * */
  public static class TestGlobalSchedulingReservationSingleNode {
    private static final Log LOG = LogFactory
        .getLog(TestGlobalSchedulingReservationSingleNode.class);
    private CapacitySchedulerConfiguration conf;

    @Before
    public void setUp() {
      CapacitySchedulerConfiguration config =
          new CapacitySchedulerConfiguration();
      config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          DominantResourceCalculator.class.getName());
      conf = new CapacitySchedulerConfiguration(config);
      conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      // Set this to avoid the AM pending issue
      conf.set(CapacitySchedulerConfiguration
          .MAXIMUM_APPLICATION_MASTERS_RESOURCE_PERCENT, "1");
      conf.setInt("yarn.scheduler.minimum-allocation-mb", 512);
      conf.setInt("yarn.scheduler.minimum-allocation-vcores", 1);
      conf.setInt("yarn.scheduler.maximum-allocation-mb", 102400);
    }

    /**
     * This is to test that big containers should be reserved when there's
     * no headroom in the cluster. When there's headroom,
     * it will allocate this container from reservation
     */
    @Test(timeout=30000)
    public void testReserveContainerAndThenAllocated() throws Exception {
      MockRM rm = new MockRM(conf);
      rm.start();
      MockNM[] nms = new MockNM[2];
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 15 * GB, 3);
      MockNM nm2 = rm.registerNode("127.0.0.2:1234", 5 * GB, 3);
      nms[0] = nm1;
      nms[1] = nm2;

      ResourceScheduler scheduler = rm.getRMContext().getScheduler();
      waitforNMRegistered(scheduler, 2, 5);

      RMApp app1 = rm.submitApp(10 * GB, "app-1", "user1", null, "default");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);

      // check node report
      SchedulerNodeReport reportNm1 =
          rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
      assertEquals(10 * GB, reportNm1.getUsedResource().getMemorySize());
      assertEquals(5 * GB,
          reportNm1.getAvailableResource().getMemorySize());

      // Reserve a Container for app2
      final AtomicBoolean result = new AtomicBoolean(false);
      RMApp app2 = rm.submitApp(10 * GB, "app-2", "user1", null, "default");
      Thread t = new Thread() {
        public void run() {
          try {
            MockRM.launchAndRegisterAM(app2, rm, nms);
            result.set(true);
          } catch (Exception e) {
            Assert.fail("Failed to Allocate Reserved Container");
          }
        }
      };
      t.start();
      Thread.sleep(1000);

      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      LeafQueue leafQueue = (LeafQueue) cs.getQueue("default");
      FiCaSchedulerApp schedulerApp = getFiCaSchedulerApp(leafQueue,
          app2.getApplicationId());
      assertEquals("App2 failed to get reserved container", 1,
          schedulerApp.getReservedContainers().size());

      // Kill app1
      rm.killApp(app1.getApplicationId());
      heartbeat(rm, nm1);

      // Check if Reserved AM of app2 gets allocated
      while (!result.get()) {
        Thread.sleep(10);
      }
      reportNm1 =
          rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
      assertEquals(10 * GB, reportNm1.getUsedResource().getMemorySize());
      assertEquals(5 * GB,
          reportNm1.getAvailableResource().getMemorySize());
      schedulerApp = getFiCaSchedulerApp(leafQueue, app2.getApplicationId());
      assertEquals("Failed to unreserve container in App2", 0,
          schedulerApp.getReservedContainers().size());
      rm.stop();
    }

    /**
     * This verifies if an appA AM container reserved on node A gets allocated
     * when node B has available space. The CS#allocateOrReserveNewContainers
     * allocates a new container to appA, CS#allocateFromReservedContainer will
     * release the reserved container from node A.
     */
    @Test(timeout=30000)
    public void testAllocateReservedAMContainerFromOtherNode() throws Exception {
      MockRM rm = new MockRM(conf);
      rm.start();
      MockNM[] nms = new MockNM[4];
      MockNM nm1 = rm.registerNode("127.0.0.1:1234", 25 * GB, 3);
      nms[0] = nm1;
      MockNM nm2 = rm.registerNode("127.0.0.2:1234", 15 * GB, 3);
      nms[1] = nm2;

      // launch an app to queue, AM container will be launched in nm1
      RMApp app1 = rm.submitApp(20 * GB, "app-1", "user1", null, "default");
      MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nm1);

      // launch an app to queue, AM container will be launched in nm2
      RMApp app2 = rm.submitApp(10 * GB, "app-2", "user1", null, "default");
      MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nm2);

      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
      RMNode rmNode1 = rm.getRMContext().getRMNodes().get(nm1.getNodeId());
      RMNode rmNode2 = rm.getRMContext().getRMNodes().get(nm2.getNodeId());

      // Reserve a Container for app3
      final AtomicBoolean result = new AtomicBoolean(false);
      Thread t = new Thread() {
        public void run() {
          try {
            RMApp app3 = rm.submitApp(10 * GB, "app-3", "user1", null, "default");
            MockAM am3 = MockRM.launchAndRegisterAM(app3, rm, nm2);
            result.set(true);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };
      t.start();
      Thread.sleep(1000);

      // Free the Space on other node where Reservation has not happened
      if (cs.getNode(rmNode1.getNodeID()).getReservedContainer() != null) {
        rm.killApp(app2.getApplicationId());
        cs.handle(new NodeUpdateSchedulerEvent(rmNode2));
      } else if (cs.getNode(rmNode2.getNodeID()).getReservedContainer() != null) {
        rm.killApp(app1.getApplicationId());
        cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
      } else {
        Assert.fail("Failed to Reserve Container");
      }

      // Check if Reserved AM of app3 gets allocated in
      // node where space available
      while (!result.get()) {
        Thread.sleep(10);
      }
      rm.stop();
    }
  }

  private static void heartbeat(MockRM rm, MockNM nm) {
    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    rm.getResourceScheduler().handle(nodeUpdate);
  }

  private static FiCaSchedulerApp getFiCaSchedulerApp(LeafQueue leafQueue,
      ApplicationId appId) {
    Iterator<FiCaSchedulerApp> apps = leafQueue.getApplications().iterator();
    FiCaSchedulerApp schedulerApp = null;
    while (apps.hasNext()) {
      FiCaSchedulerApp cur = apps.next();
      if (cur.getApplicationId().equals(appId)) {
        schedulerApp = cur;
        break;
      }
    }
    assertNotNull("Failed to find application", schedulerApp);
    return schedulerApp;
  }

}