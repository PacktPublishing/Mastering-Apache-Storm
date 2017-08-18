package com.stormadvance.logprocessing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 * This class call the ApacheLogSplitter class and pass the set of fields (ip,
 * referrer, user-agent, etc) to next bolt in Topology.
 */
public class ApacheLogSplitterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	// Create the instance of ApacheLogSplitter class.
	private static final ApacheLogSplitter apacheLogSplitter = new ApacheLogSplitter();
	private static final List<String> LOG_ELEMENTS = new ArrayList<String>();
	static {
		LOG_ELEMENTS.add("ip");
		LOG_ELEMENTS.add("dateTime");
		LOG_ELEMENTS.add("request");
		LOG_ELEMENTS.add("response");
		LOG_ELEMENTS.add("bytesSent");
		LOG_ELEMENTS.add("referrer");
		LOG_ELEMENTS.add("useragent");
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// Get the Apache log from the tuple
		String log = input.getString(0);

		if (StringUtils.isBlank(log)) {
			// ignore blank lines
			return;
		}
		// call the logSplitter(String apachelog) method of ApacheLogSplitter
		// class.
		Map<String, Object> logMap = apacheLogSplitter.logSplitter(log);
		List<Object> logdata = new ArrayList<Object>();
		for (String element : LOG_ELEMENTS) {
			logdata.add(logMap.get(element));
		}
		// emits set of fields (ip, referrer, user-agent, bytesSent, etc)
		collector.emit(logdata);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// specify the name of output fields.
		declarer.declare(new Fields("ip", "dateTime", "request", "response",
				"bytesSent", "referrer", "useragent"));
	}
}
