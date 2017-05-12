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
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.transport.GeoEventAwareTransport;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.util.Converter;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.*;

import java.nio.ByteBuffer;
import java.util.Properties;

class KafkaOutboundTransport extends OutboundTransportBase implements GeoEventAwareTransport {
  private static final BundleLogger LOGGER	= BundleLoggerFactory.getLogger(KafkaOutboundTransport.class);
  private KafkaEventProducer producer;
  private String zkConnect = "localhost:2181";
  private String bootstrap = "localhost:9092";
  private String topic;
  private int partitions;
  private int replicas;
  private String partitionKeyTag;

  KafkaOutboundTransport(TransportDefinition definition) throws ComponentException {
    super(definition);
  }

  @Override
  public synchronized void receive(final ByteBuffer byteBuffer, String channelId) {
    receive(byteBuffer, channelId, null);
  }

  @Override
  public void receive(ByteBuffer byteBuffer, String channelId, GeoEvent geoEvent) {
    try {
      if (geoEvent != null) {
        if (producer == null) {
          producer = new KafkaEventProducer(new EventDestination(topic), bootstrap);
        }

        Object partitionKey = null;

        if(partitionKeyTag != null && !partitionKeyTag.isEmpty())
        {
          final int tagIndex = geoEvent.getGeoEventDefinition().getIndexOf(partitionKeyTag);

          if (tagIndex >= 0) {
            partitionKey = geoEvent.getField(tagIndex);
          }
          else
          {
            final String warnMsg = LOGGER.translate("NO_MATCHING_TAG_WARNING",
                    geoEvent.getGeoEventDefinition()
                            .getName(),
                    partitionKeyTag);
            LOGGER.warn(warnMsg);
          }
        }

        producer.send(byteBuffer, partitionKey);
      }
    }
    catch (MessagingException e)
    {
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug(e.getMessage(), e.getCause());
      }
    }
  }

  @SuppressWarnings("incomplete-switch")
  public synchronized void start() throws RunningException {
    switch (getRunningState())
    {
      case STOPPING:
      case STOPPED:
      case ERROR:
        connect();
        break;
    }
  }

  @Override
  public synchronized void stop() {
    if (!RunningState.STOPPED.equals(getRunningState()))
      disconnect("");
  }

  @Override
  public void afterPropertiesSet() {
    super.afterPropertiesSet();
    shutdownProducer();
    zkConnect = getProperty("zkConnect").getValueAsString();
    bootstrap = getProperty("bootstrap").getValueAsString();
    topic = getProperty("topic").getValueAsString();
    partitions = Converter.convertToInteger(getProperty("partitions").getValueAsString(), 1);
    this.partitionKeyTag = getProperty("partitionKeyTag").getValueAsString();

    replicas = Converter.convertToInteger(getProperty("replicas").getValueAsString(), 0);
  }

  @Override
  public void validate() throws ValidationException {
    super.validate();
    if (zkConnect == null || zkConnect.isEmpty())
      throw new ValidationException(LOGGER.translate("ZKCONNECT_VALIDATE_ERROR"));
    if (bootstrap == null || bootstrap.isEmpty())
      throw new ValidationException(LOGGER.translate("BOOTSTRAP_VALIDATE_ERROR"));
    if (topic == null || topic.isEmpty())
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    ZkClient zkClient = new ZkClient(zkConnect, 10000, 8000, ZKStringSerializer$.MODULE$);
    // Security for Kafka was added in Kafka 0.9.0.0 -> isSecureKafkaCluster = false
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
    if (AdminUtils.topicExists(zkUtils, topic))
      zkClient.deleteRecursive(ZkUtils.getTopicPath(topic));

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try
    {

      Thread.currentThread().setContextClassLoader(null);
      AdminUtils.createTopic(zkUtils, topic, partitions, replicas, new Properties(), RackAwareMode.Disabled$.MODULE$);
    }
    catch (Throwable th) {
      LOGGER.error(th.getMessage(), th);
      throw new ValidationException(th.getMessage());
    }
    finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    zkClient.close();
  }

  private synchronized void disconnect(String reason) {
    setRunningState(RunningState.STOPPING);
    if (producer != null) {
      producer.disconnect();
      producer = null;
    }
    setErrorMessage(reason);
    setRunningState(RunningState.STOPPED);
  }

  private synchronized void connect() {
    disconnect("");
    setRunningState(RunningState.STARTED);
  }

  private synchronized void shutdownProducer() {
    if (producer != null) {
      producer.shutdown();
      producer = null;
    }
  }

  public void shutdown() {
    shutdownProducer();
    super.shutdown();
  }

  private class KafkaEventProducer extends KafkaComponentBase {
    private KafkaProducer<byte[], byte[]> producer;

    private final Callback completionCallback = new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e != null)
        {
          final String errorMsg = LOGGER.translate("KAFKA_SEND_FAILURE_ERROR", destination.getName(), e.getMessage());
          LOGGER.error(errorMsg);
          // offset = metadata.offset();
          return;
        }

        if(LOGGER.isDebugEnabled())
        {
          final String debugMsg = LOGGER.translate("KAFKA_SENT_RECORD_DEBUG",
                  metadata.topic(),
                  metadata.partition(),
                  metadata.offset(),
                  metadata.serializedKeySize(),
                  metadata.serializedValueSize());
          LOGGER.debug(debugMsg);
        }
      }
    };

    KafkaEventProducer(EventDestination destination, String bootstrap) {
      super(destination);
      // http://kafka.apache.org/documentation.html#producerconfigs
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
      props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-for-geoevent");
      // props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
      // props.put(ProducerConfig.ACKS_CONFIG, "0");
      // props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "0");
      // props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "0");
      // props.put(ProducerConfig.RETRIES_CONFIG, "0");
      try {
        setup();
      }
      catch (MessagingException e) {
        setDisconnected(e);
      }
    }

    @Override
    public synchronized void init() throws MessagingException {
      if (producer == null) {
        Thread.currentThread().setContextClassLoader(null); // see http://stackoverflow.com/questions/34734907/karaf-kafka-osgi-bundle-producer-issue for details
        producer = new KafkaProducer<byte[], byte[]>(props);
      }
    }

    void send(final ByteBuffer bb, Object partitionKey) throws MessagingException {
      // wait to send messages if we are not connected
      if (isConnected()) {
        final ProducerRecord<byte[], byte[]> record;

        if (partitionKey != null) {
          // TODO: Support serializing based on key data type instead of using hashcode (e.g. StringSerializer)
          final int h = partitionKey.hashCode();
          final byte[] key = new byte[]{
                  (byte) (h >>> 24),
                  (byte) (h >>> 16),
                  (byte) (h >>> 8),
                  (byte) h
          };
          record = new ProducerRecord<byte[], byte[]>(
                  destination.getName(),
                  key,
                  bb.array());
        } else {
          record = new ProducerRecord<byte[], byte[]>(
                  destination.getName(),
                  bb.array());
        }

        producer.send(record, completionCallback);
      }

    }

    @Override
    public synchronized void disconnect() {
      if (producer != null) {
        producer.close();
        producer = null;
      }
      super.disconnect();
    }

    @Override
    public synchronized void shutdown() {
      disconnect();
      super.shutdown();
    }
  }
}
