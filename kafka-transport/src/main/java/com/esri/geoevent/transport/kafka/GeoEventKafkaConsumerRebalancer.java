package com.esri.geoevent.transport.kafka;

import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GeoEventKafkaConsumerRebalancer implements ConsumerRebalanceListener
{

  private BundleLogger LOGGER = BundleLoggerFactory.getLogger(GeoEventKafkaConsumerRebalancer.class);

  private static GeoEventKafkaConsumerRebalancer geoEventKafkaConsumerRebalancer = null;
  private static KafkaConsumer<byte[], byte[]>   instanceKafkaConsumer;

  private GeoEventKafkaConsumerRebalancer()
  {

  }

  public static GeoEventKafkaConsumerRebalancer getInstance(KafkaConsumer<byte[], byte[]> kafkaConsumer)
  {
    if (geoEventKafkaConsumerRebalancer == null)
      geoEventKafkaConsumerRebalancer = new GeoEventKafkaConsumerRebalancer();
    instanceKafkaConsumer = kafkaConsumer;
    return geoEventKafkaConsumerRebalancer;
  }

  /**
   * In the event that a consumer is deemed dead, commit all offsets in the collection of topics
   * in the partitions.
   *
   * @param partitions
   */

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions)
  {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    partitions.forEach(topicPartition -> {
      offsets.put(topicPartition, instanceKafkaConsumer.committed(topicPartition));
      offsets.forEach((partitionOfTopic, metadataAndOffset) -> instanceKafkaConsumer.commitAsync(offsets, (offset, exception) -> {
        LOGGER.debug("COMMIT_REQUEST_COMPLETED");
        LOGGER.debug("PARTITION_REVOKED_ON_TOPIC: " + partitionOfTopic.topic());
      }));
    });
  }

  /**
   * In the event of a partition reassignment, get the last committed offset for each partition and
   * override the next offset that the consumer will use on next call to poll. Offset in this case is
   * incremented by 1. This implementation obeys the exactly once delivery semantic.
   *
   * @param partitions
   */
  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions)
  {
    partitions.forEach(topicPartition -> {
      OffsetAndMetadata offsetAndMetadata = instanceKafkaConsumer.committed(topicPartition, Duration.of(10, ChronoUnit.SECONDS));
      instanceKafkaConsumer.seek(topicPartition, offsetAndMetadata.offset() + 1);
      LOGGER.debug("PARTITION_REVOKED: " + topicPartition.partition() + " ON " + topicPartition.topic());
    });
  }

}
