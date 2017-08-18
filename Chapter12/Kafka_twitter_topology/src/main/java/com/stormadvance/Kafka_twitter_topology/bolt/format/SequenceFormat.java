package com.stormadvance.Kafka_twitter_topology.bolt.format;

import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.storm.tuple.Tuple;

/**
 * Interface for converting <code>Tuple</code> objects to HDFS sequence file key-value pairs.
 *
 */
public interface SequenceFormat extends Serializable {
    /**
     * Key class used by implementation (e.g. IntWritable.class, etc.)
     *
     * @return
     */
    Class keyClass();

    /**
     * Value class used by implementation (e.g. Text.class, etc.)
     * @return
     */
    Class valueClass();

    /**
     * Given a tuple, return the key that should be written to the sequence file.
     *
     * @param tuple
     * @return
     */
    Writable key(Tuple tuple);

    /**
     * Given a tuple, return the value that should be written to the sequence file.
     * @param tuple
     * @return
     */
    Writable value(Tuple tuple);
}
