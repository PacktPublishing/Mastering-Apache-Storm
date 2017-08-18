package com.stormadvance.logprocessing;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class contains logic to Parse an Apache log file with Regular
 * Expressions
 */
public class ApacheLogSplitter {

	public Map<String, Object> logSplitter(String apacheLog) {

		String logEntryLine = apacheLog;
		// Regex pattern to split fetch the different properties from log lines.
		String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w-:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";

		Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = p.matcher(logEntryLine);
		Map<String, Object> logMap = new HashMap<String, Object>();
		if (!matcher.matches() || 9 != matcher.groupCount()) {
			System.err.println("Bad log entry (or problem with RE?):");
			System.err.println(logEntryLine);
			return logMap;
		}
		// set the ip, dateTime, request, etc into map.
		logMap.put("ip", matcher.group(1));
		logMap.put("dateTime", matcher.group(4));
		logMap.put("request", matcher.group(5));
		logMap.put("response", matcher.group(6));
		logMap.put("bytesSent", matcher.group(7));
		logMap.put("referrer", matcher.group(8));
		logMap.put("useragent", matcher.group(9));
		return logMap;
	}

	public static void main(String[] args) {
		System.out
				.println(new ApacheLogSplitter()
						.logSplitter("98.83.179.51 - - [18/May/2011:19:35:08 -0700] \"GET /css/main.css HTTP/1.1\" 200 1837 \"http://www.safesand.com/information.htm\" \"Mozilla/5.0 (Windows NT 6.0; WOW64; rv:2.0.1) Gecko/20100101 Firefox/4.0.1\""));
	}

}
