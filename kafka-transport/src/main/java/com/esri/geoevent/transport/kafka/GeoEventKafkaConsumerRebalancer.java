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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class GeoEventKafkaConsumerRebalancer implements ConsumerRebalanceListener
{

  private boolean                       isFromBeginning;
  private KafkaConsumer<byte[], byte[]> consumer;
  private BundleLogger                  LOGGER = BundleLoggerFactory.getLogger(GeoEventKafkaConsumerRebalancer.class);


  public GeoEventKafkaConsumerRebalancer(boolean fromBeginning, KafkaConsumer<byte[], byte[]> kafkaConsumer)
  {
    isFromBeginning = fromBeginning;
    consumer = kafkaConsumer;
  }


  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions)
  {
    partitions.forEach(topicPartition -> LOGGER.info("PARTITION_REVOKED_ON_TOPIC: " + topicPartition.topic()));
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions)
  {
    if (isFromBeginning)
      consumer.seekToBeginning(partitions);
    partitions.forEach(topicPartition -> LOGGER.info("PARTITION_ASSIGNED_ON_TOPIC: " + topicPartition.topic()));
  }

}
