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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@Private
@Unstable
public class LeafQueue extends AbstractLeafQueue {
  private static final Log LOG = LogFactory.getLog(LeafQueue.class);

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public LeafQueue(CapacitySchedulerContext cs,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this(cs, cs.getConfiguration(), queueName, parent, old, false);
  }

  public LeafQueue(CapacitySchedulerContext cs,
      CapacitySchedulerConfiguration configuration,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this(cs, configuration, queueName, parent, old, false);
  }

  public LeafQueue(CapacitySchedulerContext cs,
      CapacitySchedulerConfiguration configuration,
      String queueName, CSQueue parent, CSQueue old, boolean isDynamic) throws
      IOException {
    super(cs, configuration, queueName, parent, old, isDynamic);

    setupQueueConfigs(cs.getClusterResource(), configuration);

    if(LOG.isDebugEnabled()) {
      LOG.debug("LeafQueue:" + " name=" + queueName
          + ", fullname=" + getQueuePath());
    }
  }
}
