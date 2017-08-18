package com.stormadvance.Kafka_twitter_topology.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import com.stormadvance.Kafka_twitter_topology.bolt.HdfsBolt;
import com.stormadvance.Kafka_twitter_topology.bolt.JSONParsingBolt;
import com.stormadvance.Kafka_twitter_topology.bolt.format.DefaultFileNameFormat;
import com.stormadvance.Kafka_twitter_topology.bolt.format.DelimitedRecordFormat;
import com.stormadvance.Kafka_twitter_topology.bolt.format.FileNameFormat;
import com.stormadvance.Kafka_twitter_topology.bolt.format.RecordFormat;
import com.stormadvance.Kafka_twitter_topology.bolt.rotation.FileRotationPolicy;
import com.stormadvance.Kafka_twitter_topology.bolt.rotation.FileSizeRotationPolicy;
import com.stormadvance.Kafka_twitter_topology.bolt.rotation.FileSizeRotationPolicy.Units;
import com.stormadvance.Kafka_twitter_topology.bolt.sync.CountSyncPolicy;
import com.stormadvance.Kafka_twitter_topology.bolt.sync.SyncPolicy;
import com.stormadvance.Kafka_twitter_topology.sentiments.SentimentBolt;

public class StormHDFSTopology {

	public static void main(String[] args) {
		// zookeeper hosts for the Kafka cluster
		BrokerHosts zkHosts = new ZkHosts("localhost:2181");

		// Create the KafkaSpout configuartion
		// Second argument is the topic name
		// Third argument is the zookeeper root for Kafka
		// Fourth argument is consumer group id
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "twitterData", "",
				"id7");

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
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);

		// use "|" instead of "," for field delimiter
		RecordFormat format = new DelimitedRecordFormat()
				.withFieldDelimiter(",");

		// sync the filesystem after every 1k tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);

		// rotate files when they reach 5MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f,
				Units.MB);

		FileNameFormat fileNameFormatSentiment = new DefaultFileNameFormat()
		.withPath("/sentiment-tweet/");

		HdfsBolt hdfsBolt2 = new HdfsBolt().withFsUrl("hdfs://127.0.0.1:8020")
				.withFileNameFormat(fileNameFormatSentiment).withRecordFormat(format)
				.withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);


		//builder.setBolt("HDFSBolt", hdfsBolt).shuffleGrouping("KafkaSpout");
		builder.setBolt("json", new JSONParsingBolt()).shuffleGrouping("KafkaSpout");
		
		//
		builder.setBolt("sentiment", new SentimentBolt("/home/centos/Desktop/workspace/storm_twitter/src/main/resources/AFINN-111.txt")).shuffleGrouping("json","stream2");

		//
		builder.setBolt("HDFS2", hdfsBolt2).shuffleGrouping("sentiment");

		// create an instance of LocalCluster class for executing topology in
		// local mode.
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();

		// Submit topology for execution
		cluster.submitTopology("KafkaToplogy", conf, builder.createTopology());

		try {
			// Wait for some time before exiting
			System.out.println("Waiting to consume from kafka");
			Thread.sleep(6000000);
		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}

		// kill the KafkaTopology
		cluster.killTopology("KafkaToplogy");

		// shut down the storm test cluster
		cluster.shutdown();

	}
}
