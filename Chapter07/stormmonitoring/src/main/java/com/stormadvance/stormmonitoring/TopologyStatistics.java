package com.stormadvance.stormmonitoring;

import java.util.Iterator;

import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.TopologySummary;

public class TopologyStatistics {

	public void printTopologyStatistics() {
		try {
		ThriftClient thriftClient = new ThriftClient();
		// Get the thrift client
		Client client = thriftClient.getClient();
		// Get the cluster info
		ClusterSummary clusterSummary = client.getClusterInfo();
		// Get the interator over TopologySummary class
		Iterator<TopologySummary> topologiesIterator = clusterSummary.get_topologies_iterator();
		while (topologiesIterator.hasNext()) {
			TopologySummary topologySummary = topologiesIterator.next();
			System.out.println("Topology ID: " + topologySummary.get_id());
			System.out.println("Topology Name: " + topologySummary.get_name());
			System.out.println("Number of Executors: "
					+ topologySummary.get_num_executors());
			System.out.println("Number of Tasks: " + topologySummary.get_num_tasks());
			System.out.println("Number of Workers: "
					+ topologySummary.get_num_workers());
			System.out.println("Topology status : " + topologySummary.get_status());
			System.out.println("Topology uptime in seconds: "
					+ topologySummary.get_uptime_secs());
		}
		}catch (Exception exception) {
			throw new RuntimeException("Error occure while fetching the topolgies  information");
		}
	}

}
