package com.esri.geoevent.transport.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

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
}
