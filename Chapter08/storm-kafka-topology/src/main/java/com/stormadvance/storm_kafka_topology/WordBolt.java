package com.stormadvance.storm_kafka_topology;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class WordBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -5353547217135922477L;

	// list used for aggregating the words
	private List<String> words = new ArrayList<String>();

	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("called");
		// Get the word from the tuple
		String word = input.getString(0);

		if (StringUtils.isBlank(word)) {
			// ignore blank lines
			return;
		}

		System.out.println("Recieved Word:" + word);

		// add word to current list of words
		words.add(word);

		if (word.endsWith(".")) {
			// word ends with '.' which means this is // the end of the sentence
			// publish a sentence tuple
			collector.emit(ImmutableList.of((Object) StringUtils.join(words,
					' ')));

			// reset the words list.
			words.clear();

		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// here we declare we will be emitting tuples with
		// a single field called "sentence"
		declarer.declare(new Fields("sentence"));
	}
}
