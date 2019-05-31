package com.esri.geoevent.transport.kafka;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.datastore.folder.FolderDataStore;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.manager.datastore.folder.FolderDataStoreManager;
import com.esri.ges.transport.GeoEventAwareTransport;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.util.Converter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

class KafkaOutboundTransport extends OutboundTransportBase implements GeoEventAwareTransport
{
  private static final BundleLogger           LOGGER = BundleLoggerFactory.getLogger(KafkaOutboundTransport.class);
  private              KafkaEventProducer     kafkaEventProducer;
  private              String                 bootstrap_servers;
  private              String                 topic;
  private              Integer                numberOfThreads;
  private              ExecutorService        executorService;
  private              Properties             producerProps;
  private              FolderDataStoreManager folderDataStoreManager;
  private              boolean                requireAuthentication;
  private              String                 authenticationType;
  private              String                 folderDataSourceName;
  private              String                 credentialFileName;

  KafkaOutboundTransport(TransportDefinition definition, FolderDataStoreManager folderDataStoreManager) throws ComponentException
  {
    super(definition);
    this.folderDataStoreManager = folderDataStoreManager;
  }

  @Override
  public synchronized void receive(final ByteBuffer byteBuffer, String channelId)
  {
    receive(byteBuffer, channelId, null);
  }

  @Override
  public void receive(ByteBuffer byteBuffer, String channelId, GeoEvent geoEvent)
  {
    if (channelId != null)
    {
      if (kafkaEventProducer == null)
        kafkaEventProducer = new KafkaEventProducer(byteBuffer, channelId);
    }
  }

  @SuppressWarnings("incomplete-switch")
  public synchronized void start() throws RunningException
  {
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

    requireAuthentication = Boolean.parseBoolean(getProperty(KafkaOutboundTransportDefinition.REQUIRE_AUTHENTICATION).getValueAsString());
    authenticationType = getProperty(KafkaOutboundTransportDefinition.AUTHENTICATION_TYPE).getValueAsString();
    folderDataSourceName = getProperty(KafkaOutboundTransportDefinition.FOLDER_DATA_SOURCE).getValueAsString();
    credentialFileName = getProperty(KafkaOutboundTransportDefinition.FILE_NAME).getValueAsString();

    bootstrap_servers = getProperty(KafkaOutboundTransportDefinition.BOOTSTRAP_SERVERS).getValueAsString();
    topic = getProperty(KafkaOutboundTransportDefinition.TOPIC).getValueAsString();
    numberOfThreads = Converter.convertToInteger(getProperty(KafkaOutboundTransportDefinition.NUMBER_OF_THREADS).getValueAsString(), 4);
  }

  @Override
  public void validate() throws ValidationException
  {
    super.validate();
    if (bootstrap_servers == null || bootstrap_servers.isEmpty())
      throw new ValidationException(LOGGER.translate("BOOTSTRAP_VALIDATE_ERROR"));
    if (topic == null || topic.isEmpty())
      throw new ValidationException(LOGGER.translate("TOPIC_VALIDATE_ERROR"));
    if (numberOfThreads <= 0)
      throw new ValidationException(LOGGER.translate("THREAD_VALIDATE_ERROR"));
    producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());//"org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());//"org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-for-geoevent");
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); //ensures exactly once delivery semantic, durability but with some performance cost as ACKS is set to all when this is set to true.

    //TODO Implement logic for zookeeper sasl security
    if (requireAuthentication)
    {
      if (authenticationType.isEmpty() || credentialFileName.isEmpty())
      {
        throw new ValidationException(LOGGER.translate("CREDENTIAL_DETAILS_REQUIRED"));
      }
      try
      {
        FolderDataStore folderDataStore = folderDataStoreManager.getFolderDataStore(folderDataSourceName);
        createConfigParamsFromFile(authenticationType, folderDataStore.getPath().getAbsolutePath());
      }
      catch (FileNotFoundException error)
      {
        LOGGER.error("CREDENTIAL_FILE_NOT_AVAILABLE", error.getMessage());
      }
    }
  }

  private void createConfigParamsFromFile(String authenticationType, String credentialFileLocation) throws FileNotFoundException
  {
    if (authenticationType.equalsIgnoreCase("SASL/Kerberos"))
      producerProps.put("com.sun.security.auth.module.Krb5LoginModule", "required");

    try (Scanner scanner = new Scanner(new File(credentialFileLocation)))
    {
      scanner.useDelimiter("\n");
      while (scanner.hasNext())
      {
        String readLine = scanner.next();
        String splitString[] = readLine.split("=", 0);
        producerProps.put(splitString[0], splitString[1]);
      }
    }
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

  private synchronized void connect()
  {
    disconnect("");
    setRunningState(RunningState.STARTED);
    executorService = Executors.newFixedThreadPool(numberOfThreads);
    if (kafkaEventProducer != null)
      IntStream.range(0, numberOfThreads).forEach(processingThread -> executorService.submit(kafkaEventProducer));
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

  private class KafkaEventProducer implements Runnable
  {
    private KafkaProducer<byte[], byte[]> producer;
    private ByteBuffer                    byteBuffer;
    private String                        eventChannelId;

    KafkaEventProducer(ByteBuffer eventBuffer, String channelId)
    {
      this.byteBuffer = eventBuffer;
      this.eventChannelId = channelId;
      init();
    }

    @Override
    public void run()
    {
      send(byteBuffer, eventChannelId);
    }

    public synchronized void init()
    {
      if (producer == null)
      {
        Thread.currentThread().setContextClassLoader(null); // see http://stackoverflow.com/questions/34734907/karaf-kafka-osgi-bundle-producer-issue for details
        producer = new KafkaProducer<>(producerProps);
      }
    }

    public void send(final ByteBuffer bb, String id)
    {
      // wait to send messages if we are not connected
      // if (isConnected())
      {
        //        byte[] key = new byte[4];
        //        key[3] = (byte) (h & 0xFF);
        //        key[2] = (byte) ((h >> 8) & 0xFF);
        //        key[1] = (byte) ((h >> 16) & 0xFF);
        //        key[0] = (byte) ((h >> 24) & 0xFF);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, id.getBytes(), bb.array());
        producer.send(record, (metadata, e) -> {
          if (e != null)
          {
            String errorMsg = LOGGER.translate("KAFKA_SEND_FAILURE_ERROR", topic, e.getMessage());
            LOGGER.error(errorMsg);
          }
          else
            LOGGER.debug("The offset of the record we just sent is: " + metadata.offset());
        });
      }
    }

    public synchronized void disconnect()
    {
      if (producer != null)
      {
        producer.close();
        producer = null;
      }
    }

    public synchronized void shutdown()
    {
      disconnect();
    }
  }
}
