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
