package com.stormadvance.stormhbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class Topology {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();

		List<String> zks = new ArrayList<String>();
		zks.add("127.0.0.1");

		List<String> cFs = new ArrayList<String>();
		cFs.add("personal");
		cFs.add("company");

		// set the spout class
		builder.setSpout("spout", new SampleSpout(), 2);
		// set the bolt class
		builder.setBolt("bolt", new StormHBaseBolt("user", cFs, zks, 2181), 2)
				.shuffleGrouping("spout");
		Config conf = new Config();
		conf.setDebug(true);
		// create an instance of LocalCluster class for
		// executing topology in local mode.
		LocalCluster cluster = new LocalCluster();

		// LearningStormTopolgy is the name of submitted topology.
		cluster.submitTopology("StormHBaseTopology", conf,
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
