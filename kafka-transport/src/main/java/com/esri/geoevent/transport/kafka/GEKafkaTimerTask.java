package com.esri.geoevent.transport.kafka;

import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;

import javax.xml.bind.ValidationException;
import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;

public class GEKafkaTimerTask extends TimerTask
{

  private static final BundleLogger                  LOGGER = BundleLoggerFactory.getLogger(GEKafkaTimerTask.class);
  private final        AdminClient                   adminClient;
  private final        Timer                         timer;

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
    clusterNodes.whenComplete((nodes, throwable) -> {
      if (throwable != null && throwable.getMessage().equalsIgnoreCase("Timed out waiting for a node assignment."))
      {
        try
        {
          throw new ValidationException(LOGGER.translate("PROBLEM_WITH_CLUSTER", throwable.getMessage()));
        }
        catch (ValidationException error)
        {
          LOGGER.error("PROBLEM_WITH_CLUSTER", error);
        }
      }
      long numOfNodes = nodes.stream().count();
      if (numOfNodes > 0)
      {
        this.cancel();
        timer.cancel();
        timer.purge();
      }
    });
  }
}
