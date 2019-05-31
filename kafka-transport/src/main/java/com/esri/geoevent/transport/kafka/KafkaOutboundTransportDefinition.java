package com.esri.geoevent.transport.kafka;

import com.esri.ges.core.property.LabeledValue;
import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.TransportDefinitionBase;
import com.esri.ges.transport.TransportType;

import java.util.ArrayList;
import java.util.List;

class KafkaOutboundTransportDefinition extends TransportDefinitionBase {
  private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(KafkaOutboundTransportDefinition.class);
  public static final String BOOTSTRAP_SERVERS = "kafkaServers";
  public static final String TOPIC = "topic";
  public static final String NUMBER_OF_THREADS = "numberThreads";
  public static final String TLS = "TLS";
  public static final String SASL_KERBEROS = "SASL/Kerberos";
  public static final String REQUIRE_AUTHENTICATION = "requireAuthentication";
  public static final String AUTHENTICATION_TYPE = "authenticationType";
  public static final String FOLDER_DATA_SOURCE = "folderDataSource";
  public static final String FILE_NAME = "fileName";

  KafkaOutboundTransportDefinition() {
    super(TransportType.OUTBOUND);
    try {

      List<LabeledValue> authenticationTypes = new ArrayList<>();
      authenticationTypes.add(new LabeledValue("${com.esri.geoevent.transport.kafka-transport.TLS_LBL}", TLS));
      authenticationTypes.add(new LabeledValue("${com.esri.geoevent.transport.kafka-transport.SASL_KERBEROS_LBL}", SASL_KERBEROS));


      propertyDefinitions.put(REQUIRE_AUTHENTICATION, new PropertyDefinition(REQUIRE_AUTHENTICATION, PropertyType.Boolean, false, "${com.esri.geoevent.transport.kafka-transport.REQUIRE_AUTH_LBL}", "${com.esri.geoevent.transport.kafka-transport.REQUIRE_AUTH_DESC}", true, false));
      propertyDefinitions.put(AUTHENTICATION_TYPE, new PropertyDefinition(AUTHENTICATION_TYPE, PropertyType.String, "", "${com.esri.geoevent.transport.kafka-transport.AUTH_TYPE_LBL}", "${com.esri.geoevent.transport.kafka-transport.AUTH_TYPE_DESC}", "requireAuthentication=true",false, false, authenticationTypes));


      propertyDefinitions.put(FOLDER_DATA_SOURCE, new PropertyDefinition(FOLDER_DATA_SOURCE, PropertyType.FolderDataStore, "", "${com.esri.geoevent.transport.kafka-transport.CREDENTIAL_FILE_LOCATION_LBL}", "${com.esri.geoevent.transport.kafka-transport.CREDENTIAL_FILE_LOCATION_DESC}", "requireAuthentication=true",false, false));
      propertyDefinitions.put(FILE_NAME, new PropertyDefinition(FILE_NAME, PropertyType.String, "", "${com.esri.geoevent.transport.kafka-transport.CREDENTIAL_FILE_NAME_LBL}", "${com.esri.geoevent.transport.kafka-transport.CREDENTIAL_FILE_NAME_DESC}", "requireAuthentication=true",false, false));

      propertyDefinitions.put(BOOTSTRAP_SERVERS, new PropertyDefinition(BOOTSTRAP_SERVERS, PropertyType.String, "10.53.51.128:9092", "${com.esri.geoevent.transport.kafka-transport.BOOTSTRAP_LBL}", "${com.esri.geoevent.transport.kafka-transport.BOOTSTRAP_DESC}", true, false));
      propertyDefinitions.put(TOPIC, new PropertyDefinition(TOPIC, PropertyType.String, "", "${com.esri.geoevent.transport.kafka-transport.TOPIC_LBL}", "${com.esri.geoevent.transport.kafka-transport.TOPIC_DESC}", true, false));
      propertyDefinitions.put(NUMBER_OF_THREADS, new PropertyDefinition(NUMBER_OF_THREADS, PropertyType.Integer, 4, "${com.esri.geoevent.transport.kafka-transport.NUMBER_OF_THREADS_LBL}", "${com.esri.geoevent.transport.kafka-transport.NUMBER_OF_THREADS_DESC}", false, false));
    }
    catch (PropertyException e)
    {
      String errorMsg = LOGGER.translate("TRANSPORT_OUT_INIT_ERROR", e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  @Override
  public String getName()
  {
    return "Kafka Custom";
  }

  @Override
  public String getDomain(){
    return "com.esri.geoevent.transport.outbound";
  }

  @Override
  public String getLabel()
  {
    return "${com.esri.geoevent.transport.kafka-transport.OUT_LABEL}";
  }

  @Override
  public String getDescription()
  {
    return "${com.esri.geoevent.transport.kafka-transport.OUT_DESC}";
  }
}
