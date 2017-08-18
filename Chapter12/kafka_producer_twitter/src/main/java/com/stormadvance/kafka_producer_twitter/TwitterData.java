package com.stormadvance.kafka_producer_twitter;

import java.util.Properties;

import kafka.producer.ProducerConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

public class TwitterData {

	/** The actual Twitter stream. It's set up to collect raw JSON data */
	private TwitterStream twitterStream;
	static String consumerKeyStr = "r1wFskT3q";
	static String consumerSecretStr = "fBbmp71HKbqalpizIwwwkBpKC";
	static String accessTokenStr = "298FPfE16frABXMcRIn7aUSSnNneMEPrUuZ";
	static String accessTokenSecretStr = "1LMNZZIfrAimpD004QilV1pH3PYTvM";

	public void start() {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey(consumerKeyStr);
		cb.setOAuthConsumerSecret(consumerSecretStr);
		cb.setOAuthAccessToken(accessTokenStr);
		cb.setOAuthAccessTokenSecret(accessTokenSecretStr);
		cb.setJSONStoreEnabled(true);
		cb.setIncludeEntitiesEnabled(true);
		// instance of TwitterStreamFactory
		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

		final Producer<String, String> producer = new KafkaProducer<String, String>(
				getProducerConfig());
		// topicDetails
		// new CreateTopic("127.0.0.1:2181").createTopic("twitterData", 2, 1);

		/** Twitter listener **/
		StatusListener listener = new StatusListener() {
			public void onStatus(Status status) {
				ProducerRecord<String, String> data = new ProducerRecord<String, String>(
						"twitterData", DataObjectFactory.getRawJSON(status));
				// send the data to kafka
				producer.send(data);
			}

			public void onException(Exception arg0) {
				System.out.println(arg0);
			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
			}

			public void onScrubGeo(long arg0, long arg1) {
			}

			public void onStallWarning(StallWarning arg0) {
			}

			public void onTrackLimitationNotice(int arg0) {
			}
		};

		/** Bind the listener **/
		twitterStream.addListener(listener);

		/** GOGOGO **/
		twitterStream.sample();
	}

	private Properties getProducerConfig() {

		Properties props = new Properties();

		// List of kafka borkers. Complete list of brokers is not required as
		// the producer will auto discover the rest of the brokers.
		props.put("bootstrap.servers", "localhost:9092");
		props.put("batch.size", 1);
		// Serializer used for sending data to kafka. Since we are sending
		// string,
		// we are using StringSerializer.
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		props.put("producer.type", "sync");

		return props;

	}

	public static void main(String[] args) throws InterruptedException {
		new TwitterData().start();
	}

}
