package com.stormadvance.Kafka_twitter_topology.bolt;

import java.io.Serializable;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;

public class JSONParsingBolt extends BaseRichBolt implements Serializable{

	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

	}

	public void execute(Tuple input) {
		try {
			String tweet = input.getString(0);
			Map<String, Object> map = new ObjectMapper().readValue(tweet, Map.class);
			collector.emit("stream1",new Values(tweet));
			collector.emit("stream2",new Values(map.get("text")));
			this.collector.ack(input);
		} catch (Exception exception) {
			exception.printStackTrace();
			this.collector.fail(input);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("stream1",new Fields("tweet"));
		declarer.declareStream("stream2",new Fields("text"));
	}

}
