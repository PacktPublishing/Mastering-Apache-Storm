package com.stormadvance.storm_elasticsearch;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class ESTopology {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();

		//ES Node list
		List<String> esNodes = new ArrayList<String>();
		esNodes.add("10.191.209.14");

		// set the spout class
		builder.setSpout("spout", new SampleSpout(), 2);
		// set the ES bolt class
		builder.setBolt("bolt", new ESBolt(esNodes), 2)
				.shuffleGrouping("spout");
		Config conf = new Config();
		conf.setDebug(true);
		// create an instance of LocalCluster class for
		// executing topology in local mode.
		LocalCluster cluster = new LocalCluster();

		// ESTopology is the name of submitted topology.
		cluster.submitTopology("ESTopology", conf,
				builder.createTopology());
		try {
			Thread.sleep(60000);
		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}
		System.out.println("Stopped Called : ");
		// kill the LearningStormTopology
		cluster.killTopology("StormHBaseTopology");
		// shutdown the storm test cluster
		cluster.shutdown();

	}
}
