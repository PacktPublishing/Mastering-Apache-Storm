package com.stormadvance.storm_esper;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SampleSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector spoutOutputCollector;
	
	private static final Map<Integer, String> PRODUCT = new HashMap<Integer, String>();
	static {
		PRODUCT.put(0, "A");
		PRODUCT.put(1, "B");
		PRODUCT.put(2, "C");
		PRODUCT.put(3, "D");
		PRODUCT.put(4, "E");
	}
	
	private static final Map<Integer, Double> price = new HashMap<Integer, Double>();
	static {
		price.put(0, 500.0);
		price.put(1, 100.0);
		price.put(2, 300.0);
		price.put(3, 900.0);
		price.put(4, 1000.0);
	}
	
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector spoutOutputCollector) {
		// Open the spout
		this.spoutOutputCollector = spoutOutputCollector;
	}

	public void nextTuple() {
		// Storm cluster repeatedly call this method to emit the continuous //
		// stream of tuples.
		final Random rand = new Random();
		// generate the random number from 0 to 4.
		int randomNumber = rand.nextInt(5);
		
		spoutOutputCollector.emit (new Values(PRODUCT.get(randomNumber),price.get(randomNumber),System.currentTimeMillis()));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// emits the field  firstName , lastName and companyName.
		declarer.declare(new Fields("product","price","timestamp"));
	}
}
