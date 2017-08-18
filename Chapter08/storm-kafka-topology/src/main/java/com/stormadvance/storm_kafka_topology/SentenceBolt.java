package com.stormadvance.storm_kafka_topology;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class SentenceBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 7104400131657100876L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		// get the sentence from the tuple and print it
		System.out.println("Recieved Sentence:");
		String sentence = input.getString(0);
		System.out.println("Recieved Sentence:" + sentence);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// we don't emit anything
	}
}

