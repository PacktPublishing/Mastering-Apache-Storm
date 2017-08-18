package com.stormadvance.storm_trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class TridentHelloWorldTopology {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Count", conf, buildTopology());
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}
	
	public static StormTopology buildTopology() {

		FakeTweetSpout spout = new FakeTweetSpout(10);
		TridentTopology topology = new TridentTopology();

		topology.newStream("spout1", spout)
				.shuffle()
				.each(new Fields("text", "Country"),
						new TridentUtility.TweetFilter())
				.groupBy(new Fields("Country"))
				.aggregate(new Fields("Country"), new Count(),
						new Fields("count"))
				.each(new Fields("count"), new TridentUtility.Print())
				.parallelismHint(2);

		return topology.build();
	}
}
