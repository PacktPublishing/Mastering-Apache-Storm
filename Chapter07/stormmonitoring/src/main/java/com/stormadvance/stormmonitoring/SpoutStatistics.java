package com.stormadvance.stormmonitoring;

import java.util.Iterator;
import java.util.Map;

import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.SpoutStats;
import org.apache.storm.generated.TopologyInfo;

public class SpoutStatistics {

	private static final String DEFAULT = "default";
	private static final String ALL_TIME = ":all-time";

	public void printSpoutStatistics(String topologyId) {
		try {
		ThriftClient thriftClient = new ThriftClient();
		// Get the nimbus thrift client
		Client client = thriftClient.getClient();
		// Get the information of given topology 
		TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
		Iterator<ExecutorSummary> executorStatusItr = topologyInfo
				.get_executors_iterator();
		while (executorStatusItr.hasNext()) {
			ExecutorSummary executorSummary = executorStatusItr.next();
			ExecutorStats executorStats = executorSummary.get_stats();
			if(executorStats !=null) {
			ExecutorSpecificStats executorSpecificStats = executorStats.get_specific();
			String componentId = executorSummary.get_component_id();
			// 
			if (executorSpecificStats.is_set_spout()) {
				SpoutStats spoutStats = executorSpecificStats.get_spout();
				System.out.println("Spout Id: " + componentId);
				System.out.println("Transferred: "
						+ getAllTimeStat(executorStats.get_transferred(),ALL_TIME));
				System.out.println("Total tuple emitted: "
						+ getAllTimeStat(executorStats.get_emitted(), ALL_TIME));
				System.out.println("Acked: "
						+ getAllTimeStat(spoutStats.get_acked(),
								ALL_TIME));
				System.out.println("Failed: "
						+ getAllTimeStat(spoutStats.get_failed(),
								ALL_TIME));
			}
			}
		}
		}catch (Exception exception) {
			throw new RuntimeException("Error occure while fetching the spout information : "+exception);
		}
	}

	private static Long getAllTimeStat(Map<String, Map<String, Long>> map,
			String statName) {
		if (map != null) {
			Long statValue = null;
			Map<String, Long> intermediateMap = map.get(statName);
			statValue = intermediateMap.get(DEFAULT);
			return statValue;
		}
		return 0L;
	}
}
