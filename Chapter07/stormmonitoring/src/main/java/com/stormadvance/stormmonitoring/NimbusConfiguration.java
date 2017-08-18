package com.stormadvance.stormmonitoring;

import org.apache.storm.generated.Nimbus.Client;

public class NimbusConfiguration {
	
	public void printNimbusStats() {
		try {
		ThriftClient thriftClient = new ThriftClient();
		Client client = thriftClient.getClient();
		String nimbusConiguration = client.getNimbusConf();
		System.out.println("Nimbus Configuration : "+nimbusConiguration);
		}catch(Exception exception) {
			throw new RuntimeException("Error occure while fetching the Nimbus statistics : ");
		}
	}
	
	public static void main(String[] args) {
		new NimbusConfiguration().printNimbusStats();
	}	
}
