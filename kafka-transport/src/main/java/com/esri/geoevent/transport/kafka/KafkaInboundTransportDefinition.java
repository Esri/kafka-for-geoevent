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

public class KafkaInboundTransportDefinition extends TransportDefinitionBase
{
  private static final BundleLogger LOGGER                   = BundleLoggerFactory.getLogger(KafkaInboundTransportDefinition.class);
  public static final  String       BOOTSTRAP_SERVERS        = "bootstrapServers";
  public static final  String       NUM_THREADS              = "numThreads";
  public static final  String       TOPIC                    = "topic";
  public static final  String       REQUIRE_AUTH             = "requireAuth";
  public static final  String       AUTH_TYPE                = "authType";
  public static final  String       TLS                      = "TLS";
  public static final  String       SASL_KERBEROS            = "SASL/Kerberos";
  public static final  String       CREDENTIAL_FILE_LOCATION = "credentialFileLocation";
  public static final  String       FILENAME                 = "filename";

  public KafkaInboundTransportDefinition()
  {
    super(TransportType.INBOUND);
    try
    {
      List<LabeledValue> authenticationTypes = new ArrayList<>();
      authenticationTypes.add(new LabeledValue("${com.esri.geoevent.transport.kafka-transport.TLS_LBL}", TLS));
      authenticationTypes.add(new LabeledValue("${com.esri.geoevent.transport.kafka-transport.SASL_KERBEROS_LBL}", SASL_KERBEROS));

      propertyDefinitions.put(REQUIRE_AUTH, new PropertyDefinition(REQUIRE_AUTH, PropertyType.Boolean, false, "${com.esri.geoevent.transport.kafka-transport.REQUIRE_AUTH_LBL}", "${com.esri.geoevent.transport.kafka-transport.REQUIRE_AUTH_DESC}", true, false));
      propertyDefinitions.put(AUTH_TYPE, new PropertyDefinition(AUTH_TYPE, PropertyType.String, "", "${com.esri.geoevent.transport.kafka-transport.AUTH_TYPE_LBL}", "${com.esri.geoevent.transport.kafka-transport.AUTH_TYPE_DESC}", "requireAuth=true", false, false, authenticationTypes));

      propertyDefinitions.put(CREDENTIAL_FILE_LOCATION, new PropertyDefinition(CREDENTIAL_FILE_LOCATION, PropertyType.FolderDataStore, "", "${com.esri.geoevent.transport.kafka-transport.CREDENTIAL_FILE_LOCATION_LBL}", "${com.esri.geoevent.transport.kafka-transport.CREDENTIAL_FILE_LOCATION_DESC}", "requireAuth=true", false, false));
      propertyDefinitions.put(FILENAME, new PropertyDefinition(FILENAME, PropertyType.String, "", "${com.esri.geoevent.transport.kafka-transport.CREDENTIAL_FILE_NAME_LBL}", "${com.esri.geoevent.transport.kafka-transport.CREDENTIAL_FILE_NAME_DESC}", "requireAuth=true", false, false));

      propertyDefinitions.put(BOOTSTRAP_SERVERS, new PropertyDefinition(BOOTSTRAP_SERVERS, PropertyType.String, "localhost:9092", "${com.esri.geoevent.transport.kafka-transport.BOOTSTRAP_LBL}", "${com.esri.geoevent.transport.kafka-transport.BOOTSTRAP_DESC}", true, false));
      propertyDefinitions.put(NUM_THREADS, new PropertyDefinition(NUM_THREADS, PropertyType.Integer, "1", "${com.esri.geoevent.transport.kafka-transport.NUM_THREADS_LBL}", "${com.esri.geoevent.transport.kafka-transport.NUM_THREADS_DESC}", true, false));
      propertyDefinitions.put(TOPIC, new PropertyDefinition(TOPIC, PropertyType.String, "", "${com.esri.geoevent.transport.kafka-transport.TOPIC_LBL}", "${com.esri.geoevent.transport.kafka-transport.TOPIC_DESC}", true, false));
    }
    catch (PropertyException e)
    {
      String errorMsg = LOGGER.translate("TRANSPORT_IN_INIT_ERROR", e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  @Override
  public String getName()
  {
    return "KafkaCustom";
  }

  @Override
  public String getVersion()
  {
    return "10.7.1";
  }

  @Override
  public String getDomain()
  {
    return "com.esri.geoevent.transport.inbound";
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
