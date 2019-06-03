package com.esri.geoevent.transport.kafka;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.transport.Transport;
import com.esri.ges.transport.TransportServiceBase;

public class KafkaInboundTransportService extends TransportServiceBase
{
  public KafkaInboundTransportService() {
    definition = new KafkaInboundTransportDefinition();
  }

  @Override
  public Transport createTransport() throws ComponentException {
    return new KafkaInboundTransport(definition);
  }

}
