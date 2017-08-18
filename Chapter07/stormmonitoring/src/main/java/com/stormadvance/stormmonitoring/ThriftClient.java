package com.stormadvance.stormmonitoring;


import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;

public class ThriftClient {
	// IP of the Storm UI node
	private static final String STORM_UI_NODE = "10.191.209.14";
	public Client getClient() {
		  // Set the IP and port of thrift server.
		  TSocket socket = new TSocket(STORM_UI_NODE, 6627);
		  TFramedTransport tFramedTransport = new TFramedTransport(socket);
		  TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tFramedTransport);
		  Client client = new Client(tBinaryProtocol);
		  try {
			  // Opem the connection with thrift client.
			  tFramedTransport.open();
		  }catch(Exception exception) {
			  throw new RuntimeException("Error occure while making connection with nimbus thrift server");
		  }
		  // return the Nimbus Thrift  client.
		  return client;		  
	}
}
