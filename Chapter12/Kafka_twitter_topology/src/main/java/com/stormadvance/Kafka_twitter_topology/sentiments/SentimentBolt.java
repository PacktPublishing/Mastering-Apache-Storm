package com.stormadvance.Kafka_twitter_topology.sentiments;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

/**
 * Breaks each tweet into words and calculates the sentiment of each tweet and
 * assocaites the sentiment value to the State and logs the same to the console
 * and also logs to the file.
 *
 * @author - centos
 */
public final class SentimentBolt extends BaseRichBolt {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SentimentBolt.class);
	private static final long serialVersionUID = -5094673458112825122L;
	private OutputCollector collector;
	private String path;
	public SentimentBolt(String path) {
		this.path = path;
	}
	private Map<String, Integer> afinnSentimentMap = new HashMap<String, Integer>();

	public final void prepare(final Map map,
			final TopologyContext topologyContext,
			final OutputCollector collector) {
		this.collector = collector;
		// Bolt will read the AFINN Sentiment file [which is in the classpath]
		// and stores the key, value pairs to a Map.
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line;
			while ((line = br.readLine()) != null) {
				String[] tabSplit = line.split("\t");
				afinnSentimentMap.put(tabSplit[0],
						Integer.parseInt(tabSplit[1]));
			}
			br.close();

		} catch (final IOException ioException) {
			LOGGER.error(ioException.getMessage(), ioException);
			ioException.printStackTrace();
			System.exit(1);
		}

	}

	public final void declareOutputFields(
			final OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("tweet","sentiment"));
	}

	public final void execute(final Tuple input) {
		try {
		final String tweet = (String) input.getValueByField("text");
		final int sentimentCurrentTweet = getSentimentOfTweet(tweet);
		collector.emit(new Values(tweet,sentimentCurrentTweet));
		this.collector.ack(input);
		}catch(Exception exception) {
			exception.printStackTrace();
			this.collector.fail(input);
		}
	}

	/**
	 * Gets the sentiment of the current tweet.
	 *
	 * @param status
	 *            -- Status Object.
	 * @return sentiment of the current tweet.
	 */
	private final int getSentimentOfTweet(final String text) {
		// Remove all punctuation and new line chars in the tweet.
		final String tweet = text.replaceAll("\\p{Punct}|\\n", " ")
				.toLowerCase();
		// Splitting the tweet on empty space.
		final Iterable<String> words = Splitter.on(' ').trimResults()
				.omitEmptyStrings().split(tweet);
		int sentimentOfCurrentTweet = 0;
		// Loop thru all the wordsd and find the sentiment of this tweet.
		for (final String word : words) {
			if (afinnSentimentMap.containsKey(word)) {
				sentimentOfCurrentTweet += afinnSentimentMap.get(word);
			}
		}
		LOGGER.debug("Tweet : Sentiment {} ==> {}", tweet,
				sentimentOfCurrentTweet);
		return sentimentOfCurrentTweet;
	}

}
