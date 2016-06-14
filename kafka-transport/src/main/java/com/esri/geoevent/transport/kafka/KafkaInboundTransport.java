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
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.util.Converter;
import kafka.admin.AdminUtils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.*;

class KafkaInboundTransport extends InboundTransportBase implements Runnable {
  private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(KafkaInboundTransport.class);
  private KafkaEventConsumer consumer;
  private String zkConnect;
  private int numThreads;
  private String topic;

  KafkaInboundTransport(TransportDefinition definition) throws ComponentException {
    super(definition);
  }

  public boolean isClusterable() {
    return true;
  }

  @Override
  public void run()
  {
    setErrorMessage("");
    setRunningState(RunningState.STARTED);
    while (isRunning())
    {
      try
      {
        byte[] bytes = consumer.receive();
        if (bytes != null && bytes.length > 0) {
          ByteBuffer bb = ByteBuffer.allocate(bytes.length);
          bb.put(bytes);
          bb.flip();
          byteListener.receive(bb, "");
          bb.clear();
        }
      }
      catch (MessagingException e) {
        LOGGER.error("", e);
      }
    }
  }

  @Override
  public void afterPropertiesSet() {
    zkConnect = getProperty("zkConnect").getValueAsString();
    numThreads = Converter.convertToInteger(getProperty("numThreads").getValueAsString(), 1);
    topic = getProperty("topic").getValueAsString();
    super.afterPropertiesSet();
  }

  @Override
  public void validate() throws ValidationException {
    super.validate();
    if (zkConnect == null || zkConnect.isEmpty())
      throw new ValidationException(LOGGER.translate("ZKCONNECT_VALIDATE_ERROR"));
    if (topic == null || topic.isEmpty())
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    if (numThreads < 1)
      throw new ValidationException(LOGGER.translate("NUM_THREADS_VALIDATE_ERROR"));
    ZkClient zkClient = new ZkClient(zkConnect, 10000, 8000, ZKStringSerializer$.MODULE$);
    // Security for Kafka was added in Kafka 0.9.0.0 -> isSecureKafkaCluster = false
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
    Boolean topicExists = AdminUtils.topicExists(zkUtils, topic);
    zkClient.close();
    if (!topicExists)
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
  }

  @Override
  public synchronized void start() throws RunningException {
    switch (getRunningState()) {
      case STOPPING:
      case STOPPED:
      case ERROR:
        connect();
        break;
    }
  }

  @Override
  public synchronized void stop()
  {
    if (!RunningState.STOPPED.equals(getRunningState()))
      disconnect("");
  }

  private synchronized void disconnect(String reason) {
    setRunningState(RunningState.STOPPING);
    if (consumer != null)
      consumer.setDisconnected(null);
    setErrorMessage(reason);
    setRunningState(RunningState.STOPPED);
  }

  private synchronized void connect()
  {
    disconnect("");
    setRunningState(RunningState.STARTING);
    if (consumer == null)
      consumer = new KafkaEventConsumer(new EventDestination(topic), zkConnect, numThreads);
    consumer.setConnected();
    new Thread(this).start();
  }

  private synchronized void shutdownConsumer() {
    if (consumer != null) {
      consumer.shutdown();
      consumer = null;
    }
  }

  public void shutdown() {
    shutdownConsumer();
    super.shutdown();
  }

  private class KafkaEventConsumer extends KafkaComponentBase {
    private Semaphore connectionLock;
    private final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>();
    private ConsumerConnector connector;
    private ExecutorService executor;
    private final int	numThreads;

    KafkaEventConsumer(EventDestination origin, String zkConnect, int numThreads) {
      super(origin);
      this.numThreads = numThreads;
      props.put("zookeeper.connect", zkConnect);
      props.put("group.id", destination.getName());
      props.put("zookeeper.session.timeout.ms", "400");
      props.put("zookeeper.sync.time.ms", "200");
      props.put("auto.commit.interval.ms", "1000");
      connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
      executor = Executors.newFixedThreadPool(numThreads);
      // setup the semaphore connection lock
      connectionLock = new Semaphore(1);
      int threadNumber = 0;
      for (final KafkaStream<byte[],byte[]> stream : connector.createMessageStreams(
          new HashMap<String,Integer>() {
            {
              put(destination.getName(), numThreads);
            }
          }
      ).get(destination.getName()))
        executor.submit(new KafkaQueueingConsumer(stream, threadNumber++));
    }

    public synchronized void init() throws MessagingException
    {
      ;
    }

    byte[] receive() throws MessagingException {
      // wait to receive messages if we are not connected
      if (!isConnected())
      {
        try
        {
          connectionLock.acquire(); // blocks execution until a connection has been recovered
        }
        catch (InterruptedException error)
        {
          ; // ignored
        }
      }
      byte[] bytes = null;
      try
      {
        bytes = queue.poll(100, TimeUnit.MILLISECONDS);
      }
      catch (Exception e)
      {
        ; // ignore
      }
      return bytes;
    }

    @Override
    protected void setConnected() {
      if (connectionLock.availablePermits() == 0)
        connectionLock.release();
      super.setConnected();
    }

    @Override
    protected void setDisconnected(Throwable th) {
      if (connectionLock.availablePermits() == 1)
        connectionLock.drainPermits();
      super.setDisconnected(th);
    }

    public synchronized void shutdown() {
      disconnect();
      super.shutdown();
    }

    @Override
    public synchronized void disconnect()
    {
      if (connector != null)
      {
        connector.shutdown();
        connector = null;
      }
      if (executor != null)
      {
        executor.shutdown();
        try
        {
          if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
            LOGGER.info("Timed out waiting for Kafka event consumer threads to shut down, exiting uncleanly");
        }
        catch (InterruptedException e)
        {
          LOGGER.error("Interrupted during Kafka event consumer threads shutdown, exiting uncleanly");
        }
        executor = null;
      }
      super.disconnect();
    }

    private class KafkaQueueingConsumer implements Runnable
    {
      private KafkaStream<byte[],byte[]> stream;
      private int threadNumber;

      KafkaQueueingConsumer(KafkaStream<byte[],byte[]> stream, int threadNumber) {
        this.stream = stream;
        this.threadNumber = threadNumber;
      }

      public void run() {
        LOGGER.info("Starting Kafka consuming thread #" + threadNumber);
        if (isConnected())
          for (ConsumerIterator<byte[], byte[]> it = stream.iterator(); it.hasNext();)
            queue.add(it.next().message());
        LOGGER.info("Shutting down Kafka consuming thread #" + threadNumber);
      }
    }
  }
}
