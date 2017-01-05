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

class KafkaOutboundTransportDefinition extends TransportDefinitionBase {
  private static final BundleLogger LOGGER	= BundleLoggerFactory.getLogger(KafkaOutboundTransportDefinition.class);

  KafkaOutboundTransportDefinition() {
    super(TransportType.OUTBOUND);
    try {
      propertyDefinitions.put("zkConnect", new PropertyDefinition("zkConnect", PropertyType.String, "localhost:2181", "${com.esri.geoevent.transport.kafka-transport.ZKCONNECT_LBL}", "${com.esri.geoevent.transport.kafka-transport.ZKCONNECT_DESC}", true, false));
      propertyDefinitions.put("bootstrap", new PropertyDefinition("bootstrap", PropertyType.String, "localhost:9092", "${com.esri.geoevent.transport.kafka-transport.BOOTSTRAP_LBL}", "${com.esri.geoevent.transport.kafka-transport.BOOTSTRAP_DESC}", true, false));
      propertyDefinitions.put("topic", new PropertyDefinition("topic", PropertyType.String, "", "${com.esri.geoevent.transport.kafka-transport.TOPIC_LBL}", "${com.esri.geoevent.transport.kafka-transport.TOPIC_DESC}", true, false));
      propertyDefinitions.put("partitions", new PropertyDefinition("partitions", PropertyType.Integer, "1", "${com.esri.geoevent.transport.kafka-transport.PARTITIONS_LBL}", "${com.esri.geoevent.transport.kafka-transport.PARTITIONS_DESC}", true, false));
      propertyDefinitions.put("replicas", new PropertyDefinition("replicas", PropertyType.Integer, "0", "${com.esri.geoevent.transport.kafka-transport.REPLICAS_LBL}", "${com.esri.geoevent.transport.kafka-transport.REPLICAS_DESC}", true, false));
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
    return "Kafka";
  }

  @Override
  public String getDomain()
  {
    return "com.esri.geoevent.transport.outbound";
  }

  @Override
  public String getVersion()
  {
    return "10.5.0";
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
