package com.stormadvance.Kafka_twitter_topology.bolt.format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.storm.tuple.Tuple;

/**
 * Basic <code>SequenceFormat</code> implementation that uses
 * <code>LongWritable</code> for keys and <code>Text</code> for values.
 *
 */
public class DefaultSequenceFormat implements SequenceFormat {
    private transient LongWritable key;
    private transient Text value;

    private String keyField;
    private String valueField;

    public DefaultSequenceFormat(String keyField, String valueField){
        this.keyField = keyField;
        this.valueField = valueField;
    }

    public Class keyClass() {
        return LongWritable.class;
    }

    public Class valueClass() {
        return Text.class;
    }

    public Writable key(Tuple tuple) {
        if(this.key == null){
            this.key  = new LongWritable();
        }
        this.key.set(tuple.getLongByField(this.keyField));
        return this.key;
    }

    public Writable value(Tuple tuple) {
        if(this.value == null){
            this.value = new Text();
        }
        this.value.set(tuple.getStringByField(this.valueField));
        return this.value;
    }
}
