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

import com.esri.ges.messaging.*;

import java.util.Observable;
import java.util.Properties;

abstract class KafkaComponentBase extends Observable implements EventDestinationAwareComponent {
  EventDestination destination;
  private volatile boolean connected = false;
  private String details = "";
  private int timeout = 10000;
  Properties props = new Properties();

  KafkaComponentBase(EventDestination destination) {
    this.destination = destination;
  }

  @Override
  public EventDestination getEventDestination() {
    return destination;
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  protected void setConnected() {
    connected = true;
    details = "";
  }

  protected void setDisconnected(Throwable th) {
    connected = false;
    if (th != null)
      details = th.getMessage();
  }

  @Override
  public String getStatusDetails() {
    return details;
  }

  @Override
  public synchronized void setup() throws MessagingException {
    disconnect();
    init();
    setConnected();
  }

  @Override
  public void update(Observable o, Object arg) {
    ;
  }

  @Override
  public void notifyObservers(Object event) {
    if (event != null) {
      setChanged();
      super.notifyObservers(event);
      clearChanged();
    }
  }

  @Override
  public synchronized void shutdown() {
    disconnect();
    notifyObservers(new MessagingEvent(ObservableStatus.SHUTDOWN, details, this));
  }

  @Override
  public void disconnect() {
    setDisconnected(null);
  }
}
