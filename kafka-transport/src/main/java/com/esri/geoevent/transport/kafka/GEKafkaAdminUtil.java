package com.esri.geoevent.transport.kafka;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse;

import java.util.*;

public class GEKafkaAdminUtil
{

  public static void performAdminClientValidation(Properties configProperties)
  {
    AdminClient adminClient = AdminClient.create(configProperties);
    DescribeClusterOptions describeOptions = new DescribeClusterOptions();
    describeOptions.timeoutMs(Integer.valueOf(10));

    Timer timer = new Timer("ReconnectionTimer");
    TimerTask timerTask = new GEKafkaTimerTask(adminClient, timer);
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
