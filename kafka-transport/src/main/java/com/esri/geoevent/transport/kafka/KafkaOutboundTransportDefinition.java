package com.esri.geoevent.transport.kafka;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyException;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.TransportDefinitionBase;
import com.esri.ges.transport.TransportType;

class KafkaOutboundTransportDefinition extends TransportDefinitionBase
{
  private static final BundleLogger LOGGER            = BundleLoggerFactory.getLogger(KafkaOutboundTransportDefinition.class);
  public static final  String       BOOTSTRAP_SERVERS = "kafkaServers";
  public static final  String       TOPIC             = "topic";


  KafkaOutboundTransportDefinition()
  {
    super(TransportType.OUTBOUND);
    try
    {
      propertyDefinitions.put(BOOTSTRAP_SERVERS, new PropertyDefinition(BOOTSTRAP_SERVERS, PropertyType.String, "localhost:9092", "${com.esri.geoevent.transport.kafka-transport.BOOTSTRAP_LBL}", "${com.esri.geoevent.transport.kafka-transport.BOOTSTRAP_DESC}", true, false));
      propertyDefinitions.put(TOPIC, new PropertyDefinition(TOPIC, PropertyType.String, "", "${com.esri.geoevent.transport.kafka-transport.TOPIC_LBL}", "${com.esri.geoevent.transport.kafka-transport.TOPIC_DESC}", true, false));
    }
    catch (PropertyException error)
    {
      String errorMsg = LOGGER.translate("TRANSPORT_OUT_INIT_ERROR", error.getMessage());
      LOGGER.error(errorMsg, error);
      throw new RuntimeException(errorMsg, error);
    }
  }

  @Override
  public String getName()
  {
    return "KafkaTransport";
  }

  @Override
  public String getDomain()
  {
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
