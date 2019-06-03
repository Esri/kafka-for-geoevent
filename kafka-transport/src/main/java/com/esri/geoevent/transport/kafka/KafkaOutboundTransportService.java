package com.esri.geoevent.transport.kafka;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.manager.datastore.folder.FolderDataStoreManager;
import com.esri.ges.transport.Transport;
import com.esri.ges.transport.TransportServiceBase;

public class KafkaOutboundTransportService extends TransportServiceBase
{

  public KafkaOutboundTransportService() {
    definition = new KafkaOutboundTransportDefinition();
  }

  @Override
  public Transport createTransport() throws ComponentException {
    return new KafkaOutboundTransport(definition);
  }

}
