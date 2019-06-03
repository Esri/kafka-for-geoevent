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
  }

  @Override
  public void validate() throws ValidationException
  {
    super.validate();

    if (bootStrapServers.isEmpty())
      throw new ValidationException(LOGGER.translate("ZKCONNECT_VALIDATE_ERROR"));
    if (topic.isEmpty())
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    if (numThreads < 1)
      throw new ValidationException(LOGGER.translate("NUM_THREADS_VALIDATE_ERROR"));
    configProperties = new Properties();
    configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, GEOEVENT_TRANSPORT_CONSUMER_GROUP);
    configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    GEKafkaAdminUtil.performAdminClientValidation(configProperties);
  }



  @Override
  public void start()
  {
    connect();
    setRunningState(RunningState.STARTED);
  }

  @Override
  public void stop()
  {
    disconnect("");
  }

  private void disconnect(String reason)
  {
    if (!RunningState.STOPPED.equals(getRunningState()))
    {
      setRunningState(RunningState.STOPPING);
      shutdownConsumer();
      setErrorMessage(reason);
      setRunningState(RunningState.STOPPED);
    }
  }

  private void connect()
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

  private void shutdownConsumer()
  {
    consumerList.forEach(eventConsumer -> {
      eventConsumer.shutdown();
    });
    executorService.shutdown();
    consumerList.clear();
    try
    {
      executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }

  public void shutdown()
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
      kafkaConsumer.subscribe(Collections.singleton(topic), GeoEventKafkaConsumerRebalancer.getInstance());
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
      }catch (Exception error){
        //TODO: put daemon logic here
      }
      finally
      {
        if (shutdownFlag.get())
        {
          kafkaConsumer.unsubscribe();
          kafkaConsumer.close();
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

