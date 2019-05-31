package com.esri.geoevent.transport.kafka;

import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class GeoEventKafkaConsumerRebalancer implements ConsumerRebalanceListener
{

  private BundleLogger LOGGER = BundleLoggerFactory.getLogger(GeoEventKafkaConsumerRebalancer.class);

  private static GeoEventKafkaConsumerRebalancer geoEventKafkaConsumerRebalancer = null;

  private GeoEventKafkaConsumerRebalancer()
  {

  }

  public static GeoEventKafkaConsumerRebalancer getInstance()
  {
    if (geoEventKafkaConsumerRebalancer == null)
      geoEventKafkaConsumerRebalancer = new GeoEventKafkaConsumerRebalancer();
    return geoEventKafkaConsumerRebalancer;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions)
  {
    partitions.forEach(topicPartition ->
        LOGGER.info("PARTITION_REVOKED_ON_TOPIC: " + topicPartition.topic() )
    );
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions)
  {
    partitions.forEach(topicPartition -> LOGGER.info("PARTITION_ASSIGNED_ON_TOPIC: " + topicPartition.topic()));
  }

}
