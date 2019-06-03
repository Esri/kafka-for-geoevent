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

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.GeoEventAwareTransport;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.ByteBuffer;
import java.util.Properties;

class KafkaOutboundTransport extends OutboundTransportBase implements GeoEventAwareTransport
{
  private static final BundleLogger                  LOGGER = BundleLoggerFactory.getLogger(KafkaOutboundTransport.class);
  private              KafkaEventProducer            kafkaEventProducer;
  private              String                        bootstrap_servers;
  private              String                        topic;
  private              Properties                    producerProps;
  private              KafkaProducer<byte[], byte[]> producer;

  KafkaOutboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
  }

  @Override
  public synchronized void receive(final ByteBuffer byteBuffer, String channelId)
  {
    receive(byteBuffer, channelId, null);
  }

  @Override
  public void receive(ByteBuffer byteBuffer, String channelId, GeoEvent geoEvent)
  {
    if (geoEvent != null)
    {
      if (geoEvent.getTrackId() != null)
        kafkaEventProducer.setEventPayLoad(byteBuffer, geoEvent.getTrackId());
      else
        kafkaEventProducer.setEventPayLoad(byteBuffer, null);
      kafkaEventProducer.send();
    }
  }

  public synchronized void start()
  {
    connect();
  }

  @Override
  public synchronized void stop()
  {
    if (!RunningState.STOPPED.equals(getRunningState()))
      disconnect("");
  }

  @Override
  public void afterPropertiesSet()
  {
    super.afterPropertiesSet();
    shutdownProducer();
    bootstrap_servers = getProperty(KafkaOutboundTransportDefinition.BOOTSTRAP_SERVERS).getValueAsString();
    topic = getProperty(KafkaOutboundTransportDefinition.TOPIC).getValueAsString();
  }

  @Override
  public void validate() throws ValidationException
  {
    super.validate();
    if (bootstrap_servers == null || bootstrap_servers.isEmpty())
      throw new ValidationException(LOGGER.translate("BOOTSTRAP_VALIDATE_ERROR"));
    if (topic == null || topic.isEmpty())
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-for-geoevent");
    Node[] allNodesIncluster = GEKafkaAdminUtil.listAllNodesInCluster(producerProps).toArray(new Node[] {});
    //check all the brokers in the cluster to ensure API compatibility before applying the idempotency configuration.
    if (GEKafkaAdminUtil.checkAPILevelCompatibility(allNodesIncluster))
      producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); //ensures exactly once delivery semantic, durability but with some performance cost as ACKS is set to all when this is set to true.
    GEKafkaAdminUtil.performAdminClientValidation(producerProps);
  }

  private synchronized void disconnect(String reason)
  {
    setRunningState(RunningState.STOPPING);
    if (kafkaEventProducer != null)
    {
      kafkaEventProducer.disconnect();
      kafkaEventProducer = null;
    }
    setErrorMessage(reason);
    setRunningState(RunningState.STOPPED);
  }

  private void connect()
  {
    disconnect("");
    setRunningState(RunningState.STARTED);
    kafkaEventProducer = new KafkaEventProducer();
  }

  private synchronized void shutdownProducer()
  {
    if (kafkaEventProducer != null)
    {
      kafkaEventProducer.shutdown();
      kafkaEventProducer = null;
    }
  }

  public void shutdown()
  {
    shutdownProducer();
    super.shutdown();
  }

  /**
   * A simple producer class to handle sending producer records on creation in every receive call.
   */
  private class KafkaEventProducer
  {
    private ProducerRecord<byte[], byte[]> producerRecord;

    KafkaEventProducer()
    {
      if (producer == null)
      {
        Thread.currentThread().setContextClassLoader(null);
        producer = new KafkaProducer<>(producerProps);
      }
    }

    public void setEventPayLoad(ByteBuffer byteBuffer, String eventTrackId)
    {
      if (eventTrackId == null)
        producerRecord = new ProducerRecord<>(topic, null, byteBuffer.array());
      else
        producerRecord = new ProducerRecord<>(topic, eventTrackId.getBytes(), byteBuffer.array());
    }

    public void send()
    {
      producer.send(producerRecord, (metadata, exception) -> {
        if (exception != null)
        {
          String errorMsg = LOGGER.translate("KAFKA_SEND_FAILURE_ERROR", topic, exception.getMessage());
          LOGGER.error(errorMsg);
        }
      });
    }

    public void disconnect()
    {
      if (producer != null)
      {
        producer.close();
        producer = null;
      }
    }

    public void shutdown()
    {
      disconnect();
    }
  }
}
