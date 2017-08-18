package com.stormadvance.Kafka_twitter_topology.common.rotation;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;

public interface RotationAction extends Serializable {
	void execute(FileSystem fileSystem, Path filePath) throws IOException;
}
