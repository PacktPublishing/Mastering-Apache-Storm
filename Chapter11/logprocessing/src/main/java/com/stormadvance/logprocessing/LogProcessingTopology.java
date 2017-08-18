package com.stormadvance.logprocessing;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class LogProcessingTopology {
	public static void main(String[] args) throws Exception {

		// zookeeper hosts for the Kafka cluster
		BrokerHosts zkHosts = new ZkHosts("ZK:2183");

		// Create the KafkaSpout configuartion
		// Second argument is the topic name
		// Third argument is the zookeepr root for Kafka
		// Fourth argument is consumer group id
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "apache_log", "",
				"id2");
		
		// Specify that the kafka messages are String
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		// We want to consume all the first messages in the topic everytime
		// we run the topology to help in debugging. In production, this
		// property should be false
		
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest
				.EarliestTime();

		// Now we create the topology
		TopologyBuilder builder = new TopologyBuilder();

		// set the kafka spout class
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 2);

		// set the LogSplitter, IpToCountry, Keyword and PersistenceBolt bolts
		// class.
		builder.setBolt("LogSplitter", new ApacheLogSplitterBolt(), 1)
				.globalGrouping("KafkaSpout");
		
		builder.setBolt(
				"IpToCountry",
				new UserInformationGetterBolt(
						args[0]), 1)
				.globalGrouping("LogSplitter");
		builder.setBolt("Keyword", new KeyWordIdentifierBolt(), 1)
				.globalGrouping("IpToCountry");
		builder.setBolt("PersistenceBolt",
				new PersistenceBolt(args[1], args[2], args[3], args[4]),
				1).globalGrouping("Keyword");

		if (args.length == 6) {
			// Run the topology on remote cluster.
			Config conf = new Config();
			conf.setNumWorkers(4);
			try {
				StormSubmitter.submitTopology(args[4], conf,
						builder.createTopology());
			} catch (AlreadyAliveException alreadyAliveException) {
				System.out.println(alreadyAliveException);
			} catch (InvalidTopologyException invalidTopologyException) {
				System.out.println(invalidTopologyException);
			}
		} else {
			// create an instance of LocalCluster class for executing topology
			// in local mode.
			LocalCluster cluster = new LocalCluster();
			Config conf = new Config();
			conf.setDebug(true);
			// Submit topology for execution
			cluster.submitTopology("KafkaToplogy1", conf,
					builder.createTopology());

			try {
				// Wait for sometime before exiting
				System.out
						.println("**********************Waiting to consume from kafka");
				Thread.sleep(100000);
				System.out.println("Stopping the sleep thread");

			} catch (Exception exception) {
				System.out
						.println("******************Thread interrupted exception : "
								+ exception);
			}

			// kill the KafkaTopology
			cluster.killTopology("KafkaToplogy1");

			// shutdown the storm test cluster
			cluster.shutdown();

		}

	}
}
