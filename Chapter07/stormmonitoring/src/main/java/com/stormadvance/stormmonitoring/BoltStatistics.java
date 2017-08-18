package com.stormadvance.stormmonitoring;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.storm.generated.BoltStats;
import org.apache.storm.generated.ExecutorSpecificStats;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.TopologyInfo;

public class BoltStatistics {

	private static final String DEFAULT = "default";
	private static final String ALL_TIME = ":all-time";

	public void printBoltStatistics(String topologyId) {

		try {
			ThriftClient thriftClient = new ThriftClient();
			// Get the Nimbus thrift server client
			Client client = thriftClient.getClient();
			
			// Get the information of given topology
			TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
			Iterator<ExecutorSummary> executorStatusItr = topologyInfo
					.get_executors_iterator();
			while (executorStatusItr.hasNext()) {
				// get the executor
				ExecutorSummary executorSummary = executorStatusItr.next();
				ExecutorStats executorStats = executorSummary.get_stats();
				if (executorStats != null) {
					ExecutorSpecificStats executorSpecificStats = executorStats
							.get_specific();
					String componentId = executorSummary.get_component_id();
					if (executorSpecificStats.is_set_bolt()) {
						BoltStats boltStats = executorSpecificStats.get_bolt();
						System.out.println("Bolt Id: " + componentId);
						System.out.println("Transferred: "
								+ getAllTimeStat(
										executorStats.get_transferred(),
										ALL_TIME));
						System.out.println("Emitted: "
								+ getAllTimeStat(executorStats.get_emitted(),
										ALL_TIME));
						System.out.println("Acked: "
								+ getBoltStatLongValueFromMap(
										boltStats.get_acked(), ALL_TIME));
						System.out.println("Failed: "
								+ getBoltStatLongValueFromMap(
										boltStats.get_failed(), ALL_TIME));
						System.out.println("Executed : "
								+ getBoltStatLongValueFromMap(
										boltStats.get_executed(), ALL_TIME));
					}
				}
			}
		} catch (Exception exception) {
			throw new RuntimeException("Error occure while fetching the bolt information :"+exception);
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

	public static Long getBoltStatLongValueFromMap(
			Map<String, Map<GlobalStreamId, Long>> map, String statName) {
		if (map != null) {
			Long statValue = null;
			Map<GlobalStreamId, Long> intermediateMap = map.get(statName);
			Set<GlobalStreamId> key = intermediateMap.keySet();
			if (key.size() > 0) {
				Iterator<GlobalStreamId> itr = key.iterator();
				statValue = intermediateMap.get(itr.next());
			}
			return statValue;
		}
		return 0L;
	}

}
