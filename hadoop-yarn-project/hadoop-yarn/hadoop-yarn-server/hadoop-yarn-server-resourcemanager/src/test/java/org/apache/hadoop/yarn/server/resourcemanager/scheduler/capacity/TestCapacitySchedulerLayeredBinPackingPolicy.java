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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.LayeredNodeUsageBinPackingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSorter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TestCapacitySchedulerLayeredBinPackingPolicy extends CapacitySchedulerTestBase {

  private static final Log LOG = LogFactory
      .getLog(TestCapacitySchedulerLayeredBinPackingPolicy.class);
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
  }

  /**
   * Test the scorer output given a usage number.
   * */
  @Test
  public void testLayeredResourceUsageScorer() {
    LayeredNodeUsageBinPackingPolicy policy = new LayeredNodeUsageBinPackingPolicy(true);
    // threshold 0.6
    float threshold = (float)0.6;
    float currentUsage = (float)0.2;
    float normalNodeScore = policy.scorer(currentUsage, threshold);
    // node with resource usage 0.2
    Assert.assertEquals(2000 + 3 * currentUsage * 100,
        normalNodeScore, 0);
    // node with resource usage 0.5
    currentUsage = (float)0.5;
    Assert.assertEquals(2000 + 3 * currentUsage * 100,
        policy.scorer(currentUsage, threshold), 0);
    // node with resource usage 0.6
    currentUsage = (float)0.6;
    Assert.assertEquals(1000 - 3 * currentUsage * 100,
        policy.scorer(currentUsage, threshold), 0);
    // node with resource usage 0.7
    currentUsage = (float)0.7;
    float busyNodeScore = policy.scorer(currentUsage, threshold);
    Assert.assertEquals(1000 - 3 * currentUsage * 100,
        policy.scorer(currentUsage, threshold), 0);
    // node with resource usage 0.9
    currentUsage = (float)0.9;
    float busyNodeScore2 = policy.scorer(currentUsage, threshold);
    Assert.assertEquals(1000 - 3 * currentUsage * 100,
        policy.scorer(currentUsage, threshold), 0);
    // node with resource usage 0, the score shouldn't exceed 100
    currentUsage = (float)0.0;
    float freeNodeScore = policy.scorer(currentUsage, threshold);
    if ( freeNodeScore >= 100.0) {
      Assert.assertFalse(
          "Node usage 0 shoudn't have a score higher than 100. Actual:"
              + freeNodeScore,
          true);
    } else {
      Assert.assertTrue(true);
    }

    Assert.assertTrue(
        "High usage busy node score should be smaller to avoid hot spot",
        busyNodeScore2 < busyNodeScore);
    Assert.assertTrue(
        "Busy node score should be smaller than normal node",
        busyNodeScore < normalNodeScore);
    Assert.assertTrue(
        "Busy node score should be larger than free node",
        busyNodeScore > freeNodeScore);
  }

  private void heartbeat(MockRM rm, MockNM nm) {
    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    rm.getResourceScheduler().handle(nodeUpdate);
  }

}