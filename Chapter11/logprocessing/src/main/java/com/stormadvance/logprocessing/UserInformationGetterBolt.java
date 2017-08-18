package com.stormadvance.logprocessing;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * This class use the IpToCountryConverter and UserAgentTools class to calculate
 * the country, os and browser from log line.
 * 
 */
public class UserInformationGetterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private IpToCountryConverter ipToCountryConverter = null;
	private UserAgentTools userAgentTools = null;
	public OutputCollector collector;
	private String pathTOGeoLiteCityFile;

	public UserInformationGetterBolt(String pathTOGeoLiteCityFile) {
		// set the path of GeoLiteCity.dat file.
		this.pathTOGeoLiteCityFile = pathTOGeoLiteCityFile;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ip", "dateTime", "request", "response",
				"bytesSent", "referrer", "useragent", "country", "browser",
				"os"));
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.ipToCountryConverter = new IpToCountryConverter(
				this.pathTOGeoLiteCityFile);
		this.userAgentTools = new UserAgentTools();

	}

	public void execute(Tuple input) {

		String ip = input.getStringByField("ip").toString();
		
		// calculate the country from ip
		Object country = ipToCountryConverter.ipToCountry(ip);
		// calculate the browser from useragent.
		Object browser = userAgentTools.getBrowser(input.getStringByField(
				"useragent").toString())[1];
		// calculate the os from useragent.
		Object os = userAgentTools.getOS(input.getStringByField("useragent")
				.toString())[1];
		collector.emit(new Values(input.getString(0), input.getString(1), input
				.getString(2), input.getString(3), input.getString(4), input
				.getString(5), input.getString(6), country, browser, os));

	}
}
