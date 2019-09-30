/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSorter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for Multi Node scheduling related tests.
 */
public class TestCapacitySchedulerMultiNodes extends CapacitySchedulerTestBase {

  private static final Log LOG = LogFactory
      .getLog(TestCapacitySchedulerMultiNodes.class);
  private CapacitySchedulerConfiguration conf;
  private static final String POLICY_CLASS_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.BinPackingNodeLookupPolicy";

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
    conf.setInt("yarn.scheduler.minimum-allocation-mb", 512);
    conf.setInt("yarn.scheduler.minimum-allocation-vcores", 1);
  }

  @Test
  public void testMultiNodeSorterForScheduling() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    rm.registerNode("127.0.0.1:1234", 10 * GB);
    rm.registerNode("127.0.0.1:1235", 10 * GB);
    rm.registerNode("127.0.0.1:1236", 10 * GB);
    rm.registerNode("127.0.0.1:1237", 10 * GB);
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 4, 5);
    MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
        .getMultiNodeSortingManager();
    MultiNodeSorter<SchedulerNode> sorter = mns
        .getMultiNodePolicy(POLICY_CLASS_NAME);
    sorter.reSortClusterNodes();
    Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    Assert.assertEquals(4, nodes.size());
    rm.stop();
  }

  // Zhankun
  @Test
  public void testMultiNodeSorterForSchedulingWithOrdering() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM[] nms = new MockNM[4];
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5 * GB, 100);
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
    Assert.assertEquals(4, nodes.size());

    RMApp app1 = rm.submitApp(5 * GB, "app-1", "user1", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);
    SchedulerNodeReport reportNm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());

    // check node report
    Assert.assertEquals(5 * GB, reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals((5 - 5) * GB,
        reportNm1.getAvailableResource().getMemorySize());

    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();

    RMApp app2 = rm.submitApp(3 * GB, "app-2", "user2", null, "default");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nms);
    Resource cResource = Resources.createResource(1 * GB, 1);
    am2.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);
    heartbeat(rm, nm2);
    // check node report
    SchedulerNodeReport reportNm2 =
        rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
    Assert.assertEquals(4 * GB, reportNm2.getUsedResource().getMemorySize());
    Assert.assertEquals((100 - 4) * GB,
        reportNm2.getAvailableResource().getMemorySize());

    // check node report
    reportNm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(5 * GB, reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals(0 * GB,
        reportNm1.getAvailableResource().getMemorySize());

    // am request containers
    cResource = Resources.createResource(8 * GB, 1);
    am1.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);

    heartbeat(rm, nm1);
    heartbeat(rm, nm2);
    heartbeat(rm, nm3);
    heartbeat(rm, nm4);

    // check node report
    reportNm2 =
        rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
    Assert.assertEquals(12 * GB, reportNm2.getUsedResource().getMemorySize());
    Assert.assertEquals((100 - 12) * GB,
        reportNm2.getAvailableResource().getMemorySize());

    SchedulerNodeReport reportNm3 =
        rm.getResourceScheduler().getNodeReport(nm3.getNodeId());
    // check node report
    Assert.assertEquals(0 * GB, reportNm3.getUsedResource().getMemorySize());
    Assert.assertEquals(100 * GB,
        reportNm3.getAvailableResource().getMemorySize());

    SchedulerNodeReport reportNm4 =
        rm.getResourceScheduler().getNodeReport(nm4.getNodeId());
    // check node report
    Assert.assertEquals(0 * GB, reportNm4.getUsedResource().getMemorySize());
    Assert.assertEquals(100 * GB,
        reportNm4.getAvailableResource().getMemorySize());

    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();

    // Node1 and Node2 are now having used resources. Hence ensure these 2 comes
    // first in the list.
    nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    List<NodeId> currentNodes = new ArrayList<>();
    currentNodes.add(nm2.getNodeId());
    currentNodes.add(nm1.getNodeId());
    currentNodes.add(nm3.getNodeId());
    currentNodes.add(nm4.getNodeId());
    Iterator<SchedulerNode> it = nodes.iterator();
    SchedulerNode current;
    int i = 0;
    while (it.hasNext()) {
      current = it.next();
      Assert.assertEquals(current.getNodeID(), currentNodes.get(i++));
    }
    rm.stop();
  }
  private void heartbeat(MockRM rm, MockNM nm) {
    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    rm.getResourceScheduler().handle(nodeUpdate);
  }
}
