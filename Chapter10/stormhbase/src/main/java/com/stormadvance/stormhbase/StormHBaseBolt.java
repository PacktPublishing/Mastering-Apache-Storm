package com.stormadvance.stormhbase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class StormHBaseBolt implements IBasicBolt {

	private static final long serialVersionUID = 2L;
	private HBaseOperations hbaseOperations;
	private String tableName;
	private List<String> columnFamilies;
	private List<String> zookeeperIPs;
	private int zkPort;
	/**
	 * Constructor of StormHBaseBolt class
	 * 
	 * @param tableName
	 *            HBaseTableNam
	 * @param columnFamilies
	 *            List of column families
	 * @param zookeeperIPs
	 *            List of zookeeper nodes
	 * @param zkPort
	 *            Zookeeper client port
	 */
	public StormHBaseBolt(String tableName, List<String> columnFamilies,
			List<String> zookeeperIPs, int zkPort) {
		this.tableName =tableName;
		this.columnFamilies = columnFamilies;
		this.zookeeperIPs = zookeeperIPs;
		this.zkPort = zkPort;

	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		Map<String, Map<String, Object>> record = new HashMap<String, Map<String, Object>>();
		Map<String, Object> personalMap = new HashMap<String, Object>();
		// "firstName","lastName","companyName")
		personalMap.put("firstName", input.getValueByField("firstName"));
		personalMap.put("lastName", input.getValueByField("lastName"));

		Map<String, Object> companyMap = new HashMap<String, Object>();
		companyMap.put("companyName", input.getValueByField("companyName"));

		record.put("personal", personalMap);
		record.put("company", companyMap);
		// call the inset method of HBaseOperations class to insert record into
		// HBase
		hbaseOperations.insert(record, UUID.randomUUID().toString());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// create the instance of HBaseOperations class
		hbaseOperations = new HBaseOperations(tableName, columnFamilies,
				zookeeperIPs, zkPort);
	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
