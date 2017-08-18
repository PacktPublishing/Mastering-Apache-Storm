package com.stormadvance.storm_esper;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class EsperTopology {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();

		// set the spout class
		builder.setSpout("spout", new SampleSpout(), 2);
		// set the ES bolt class
		builder.setBolt("bolt", new EsperBolt(), 2)
				.shuffleGrouping("spout");
		Config conf = new Config();
		conf.setDebug(true);
		// create an instance of LocalCluster class for
		// executing topology in local mode.
		LocalCluster cluster = new LocalCluster();

		// EsperTopology is the name of submitted topology.
		cluster.submitTopology("EsperTopology", conf,
				builder.createTopology());
		try {
			Thread.sleep(60000);
		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}
		System.out.println("Stopped Called : ");
		// kill the LearningStormTopology
		cluster.killTopology("EsperTopology");
		// shutdown the storm test cluster
		cluster.shutdown();

	}
}
