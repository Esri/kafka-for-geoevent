/*
  Copyright 1995-2016 Esri
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373
  email: contracts@esri.com
*/


package com.esri.geoevent.transport.kafka;

import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;

import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

public class GEKafkaTimerTask extends TimerTask
{

  private static final BundleLogger LOGGER          = BundleLoggerFactory.getLogger(GEKafkaTimerTask.class);
  private              AdminClient  adminClient;
  private              Timer        timer;

  public GEKafkaTimerTask(AdminClient adminClient, Timer timer)
  {
    this.adminClient = adminClient;
    this.timer = timer;
  }

  @Override
  public void run()
  {
    DescribeClusterResult clusterDescription = adminClient.describeCluster();
    KafkaFuture<Collection<Node>> clusterNodes = clusterDescription.nodes();
    try
    {

      long count = clusterNodes.get().stream().count();
      if (count > 0)
      {
        cancel();
        timer.cancel();
        timer.purge();
      }
    }
    catch (InterruptedException | ExecutionException intEx)
    {
      LOGGER.error("PROBLEM_WITH_CLUSTER", intEx);
    }

  }
}
