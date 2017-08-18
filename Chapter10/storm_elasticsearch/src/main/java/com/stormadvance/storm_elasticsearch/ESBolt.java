package com.stormadvance.storm_elasticsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class ESBolt implements IBasicBolt {

	private static final long serialVersionUID = 2L;
	private ElasticSearchOperation elasticSearchOperation;
	private List<String> esNodes;

	/**
	 * 
	 * @param esNodes
	 */
	public ESBolt(List<String> esNodes) {
		this.esNodes = esNodes;

	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		Map<String, Object> personalMap = new HashMap<String, Object>();
		// "firstName","lastName","companyName")
		personalMap.put("firstName", input.getValueByField("firstName"));
		personalMap.put("lastName", input.getValueByField("lastName"));

		personalMap.put("companyName", input.getValueByField("companyName"));
		elasticSearchOperation.insert(personalMap,"person","personmapping",UUID.randomUUID().toString());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		try {
			// create the instance of ESOperations class
			elasticSearchOperation = new ElasticSearchOperation(esNodes);
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	public void cleanup() {

	}

}
