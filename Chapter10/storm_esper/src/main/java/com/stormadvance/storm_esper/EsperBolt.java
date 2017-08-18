package com.stormadvance.storm_esper;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class EsperBolt implements IBasicBolt {

	private static final long serialVersionUID = 2L;
	private EsperOperation esperOperation;

	public EsperBolt() {
	

	}

	
	public void execute(Tuple input, BasicOutputCollector collector) {
	
		double price = input.getDoubleByField("price");
		long timeStamp = input.getLongByField("timestamp");
		//long timeStamp = System.currentTimeMillis();
		String product = input.getStringByField("product");
		Stock stock = new Stock(product, price, timeStamp);
		esperOperation.esperPut(stock);
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
			esperOperation = new EsperOperation();
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	public void cleanup() {

	}
}
