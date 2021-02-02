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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.*;

public class GEKafkaAdminUtil
{

  public static void performAdminClientValidation(Properties configProperties)
  {
    AdminClient adminClient = AdminClient.create(configProperties);
    DescribeClusterOptions describeOptions = new DescribeClusterOptions();
    describeOptions.timeoutMs(Integer.valueOf(10));
    Timer timer = new Timer("ReconnectionTimer");
    GEKafkaTimerTask timerTask = new GEKafkaTimerTask(adminClient, timer);
    timer.scheduleAtFixedRate(timerTask, 0L, 10000L);
  }

  public static boolean checkAPILevelCompatibility(Node... kafkaNodes)
  {
    ApiVersions apiVersions = new ApiVersions();
    ArrayList<Short> apiVersionCollection = new ArrayList<>();
    ApiKeys apiKeys = ApiKeys.forId(0);
    Arrays.stream(kafkaNodes).forEach(eachNode -> apiVersionCollection.add(apiVersions.get(eachNode.idString()).latestUsableVersion(apiKeys)));
    long count = apiVersionCollection.stream().filter(predicate -> predicate.intValue() > ((short) 0.11)).count();
    return count > 0L ? true : false;
  }

  public static synchronized Collection<Node> listAllNodesInCluster(Properties configProperties)
  {
    AdminClient adminClient = AdminClient.create(configProperties);
    DescribeClusterOptions describeOptions = new DescribeClusterOptions();
    describeOptions.timeoutMs(Integer.valueOf(10));
    DescribeClusterResult describedCluster = adminClient.describeCluster(describeOptions);
    ArrayList<Node> nodeArrayList = new ArrayList<>();
    describedCluster.nodes().whenComplete((nodes, throwable) -> nodeArrayList.addAll(nodes));
    return nodeArrayList;
  }
}
