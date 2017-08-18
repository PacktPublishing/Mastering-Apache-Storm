package com.stormadvance.storm_trident;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.DRPCClient;

public class DistributedRPC {
		
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		LocalDRPC drpc = new LocalDRPC();
		if (args.length == 0) {
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("CountryCount", conf, buildTopology(drpc));
			Thread.sleep(2000);
			for(int i=0; i<100 ; i++) {
				System.out.println("Result - "+drpc.execute("Count", "Japan India Europe"));
				Thread.sleep(1000);
				}
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
			Thread.sleep(2000);
   		  	DRPCClient client = new DRPCClient(conf, "RRPC-Server", 1234);
   		  	System.out.println(client.execute("Count", "Japan India Europe"));
		}
	}

	
	public static StormTopology buildTopology(LocalDRPC drpc) {

		FakeTweetSpout spout = new FakeTweetSpout(10);
		TridentTopology topology = new TridentTopology();
		TridentState countryCount = topology.newStream("spout1", spout)
				.shuffle()
				.each(new Fields("text","Country"), new TridentUtility.TweetFilter()).groupBy(new Fields("Country"))
				.persistentAggregate(new MemoryMapState.Factory(),new Fields("Country"), new Count(), new Fields("count"))
				.parallelismHint(2);
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		
		topology.newDRPCStream("Count", drpc)
		.each(new Fields("args"), new TridentUtility.Split(), new Fields("Country"))					
		.stateQuery(countryCount, new Fields("Country"), new MapGet(),
				new Fields("count")).each(new Fields("count"),
				        new FilterNull());
		
		return topology.build();
	}
}
