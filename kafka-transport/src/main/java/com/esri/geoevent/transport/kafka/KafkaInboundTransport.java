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
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.util.Converter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

class KafkaInboundTransport extends InboundTransportBase
{
  private static final BundleLogger             LOGGER                            = BundleLoggerFactory.getLogger(KafkaInboundTransport.class);
  public static final  String                   GEOEVENT_TRANSPORT_CONSUMER_GROUP = "geoevent-transport-consumer-group";
  private              String                   bootStrapServers;
  private              int                      numThreads;
  private              String                   topic;
  private              Properties               configProperties;
  private              AtomicBoolean            shutdownFlag                      = new AtomicBoolean(false);
  private              ExecutorService          executorService;
  private              List<KafkaEventConsumer> consumerList                      = new ArrayList<>();
  private              String                   consumerGroupId;
  private              boolean                  fromBeginning;

  KafkaInboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
  }

  public boolean isClusterable()
  {
    return true;
  }

  @Override
  public void afterPropertiesSet()
  {
    super.afterPropertiesSet();
    bootStrapServers = getProperty(KafkaInboundTransportDefinition.BOOTSTRAP_SERVERS).getValueAsString();
    numThreads = Converter.convertToInteger(getProperty(KafkaInboundTransportDefinition.NUM_THREADS).getValueAsString(), 1);
    topic = getProperty(KafkaInboundTransportDefinition.TOPIC).getValueAsString();
    consumerGroupId = getProperty(KafkaInboundTransportDefinition.CONSUMER_GROUP_ID).getValueAsString();
    fromBeginning = Boolean.parseBoolean(getProperty(KafkaInboundTransportDefinition.SEEK_FROM_BEGINNING).getValueAsString());
  }

  @Override
  public void validate() throws ValidationException
  {
    super.validate();

    if (bootStrapServers.isEmpty())
      throw new ValidationException(LOGGER.translate("BOOTSTRAP_VALIDATE_ERROR"));
    if (topic.isEmpty())
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    if (numThreads < 1)
      throw new ValidationException(LOGGER.translate("NUM_THREADS_VALIDATE_ERROR"));
    configProperties = new Properties();
    configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    if (consumerGroupId.isEmpty())
      configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, GEOEVENT_TRANSPORT_CONSUMER_GROUP);
    else
      configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    GEKafkaAdminUtil.performAdminClientValidation(configProperties);
  }

  @Override
  public synchronized void start()
  {
    connect();
    setRunningState(RunningState.STARTED);
  }

  @Override
  public synchronized void stop()
  {
    disconnect("");
  }

  private synchronized void disconnect(String reason)
  {
    if (!RunningState.STOPPED.equals(getRunningState()))
    {
      setRunningState(RunningState.STOPPING);
      shutdownConsumer();
      setErrorMessage(reason);
      setRunningState(RunningState.STOPPED);
    }
  }

  private synchronized void connect()
  {
    setRunningState(RunningState.STARTING);
    shutdownFlag.set(false);
    executorService = Executors.newFixedThreadPool(numThreads);
    IntStream.range(0, numThreads).forEach(processingThread -> {
      KafkaEventConsumer kafkaEventConsumer = new KafkaEventConsumer();
      consumerList.add(kafkaEventConsumer);
      executorService.submit(kafkaEventConsumer);
    });
  }

  private synchronized void shutdownConsumer()
  {
    consumerList.forEach(eventConsumer -> {
      eventConsumer.shutdown();
    });
    executorService.shutdown();
    try
    {
      executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
    }
    catch (InterruptedException error)
    {
      LOGGER.error("THREAD_SHUTDOWN_ERROR", error);
    }
    consumerList.clear();
  }

  public synchronized void shutdown()
  {
    super.shutdown();
    shutdownConsumer();
  }

  private class KafkaEventConsumer implements Runnable
  {
    private final BlockingQueue<byte[]>         queue = new LinkedBlockingQueue<>();
    private       KafkaConsumer<byte[], byte[]> kafkaConsumer;

    KafkaEventConsumer()
    {
      Thread.currentThread().setContextClassLoader(null);
      kafkaConsumer = new KafkaConsumer<>(configProperties);
      kafkaConsumer.subscribe(Collections.singleton(topic), new GeoEventKafkaConsumerRebalancer(fromBeginning, kafkaConsumer));
    }

    @Override
    public void run()
    {
      try
      {
        while (!shutdownFlag.get())
        {
          ConsumerRecords<byte[], byte[]> pollBytes = kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
          pollBytes.forEach(polledConsumerRecord -> queue.offer(polledConsumerRecord.value()));
          if (!queue.isEmpty())
            this.sendBytesToAdapter();
          kafkaConsumer.commitAsync();
        }
      }
      catch (KafkaException | IllegalArgumentException | IllegalStateException state)
      {
        LOGGER.error("POLL_PROBLEM", state);
        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
      }
      finally
      {
        if (shutdownFlag.get())
        {
          if (kafkaConsumer != null)
          {
            kafkaConsumer.unsubscribe();
            kafkaConsumer.close();
          }
        }
      }
    }

    private void sendBytesToAdapter()
    {
      while (!queue.isEmpty())
      {
        byte[] bytes = this.receive();
        if (bytes != null && bytes.length > 0)
        {
          ByteBuffer bb = ByteBuffer.allocate(bytes.length);
          bb.put(bytes);
          bb.flip();
          byteListener.receive(bb, "");
          bb.clear();
        }
      }
    }

    private byte[] receive()
    {
      byte[] bytes = null;
      try
      {
        bytes = queue.poll(100, TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException interruptedException)
      {
        LOGGER.error("QUEUE_POLL_INTERRUPTED", interruptedException.getLocalizedMessage());
      }
      return bytes;
    }

    public void shutdown()
    {
      shutdownFlag.set(true);
    }

  }
}

