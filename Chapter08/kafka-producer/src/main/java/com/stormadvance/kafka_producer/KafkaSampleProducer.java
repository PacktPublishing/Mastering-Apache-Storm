package com.stormadvance.kafka_producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSampleProducer {
	public static void main(String[] args) {
		// Build the configuration required for connecting to Kafka
		Properties props = new Properties();

		// List of kafka borkers. Complete list of brokers is not required as
		// the producer will auto discover the rest of the brokers.
		props.put("bootstrap.servers", "10.191.208.89:9092");
		props.put("batch.size", 1);
		// Serializer used for sending data to kafka. Since we are sending string,
		// we are using StringSerializer.
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		props.put("producer.type", "sync");
		
		// Create the producer instance
		Producer<String, String> producer = new KafkaProducer<String, String>(
				props);

		// Now we break each word from the paragraph
		for (String word : METAMORPHOSIS_OPENING_PARA.split("\\s")) {
			System.out.println("word : " + word);
			// Create message to be sent to "new_topic" topic with the word
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(
					"new_topic",word, word);
			// Send the message
			producer.send(data);
		}

		// close the producer
		producer.close();
		System.out.println("end : ");
	}

	// First paragraph from Franz Kafka's Metamorphosis
	private static String METAMORPHOSIS_OPENING_PARA = "One morning, when Gregor Samsa woke from troubled dreams, he found "
			+ "himself transformed in his bed into a horrible vermin.  He lay on "
			+ "his armour-like back, and if he lifted his head a little he could "
			+ "see his brown belly, slightly domed and divided by arches into stiff "
			+ "sections.  The bedding was hardly able to cover it and seemed ready "
			+ "to slide off any moment.  His many legs, pitifully thin compared "
			+ "with the size of the rest of him, waved about helplessly as he "
			+ "looked.";

}
