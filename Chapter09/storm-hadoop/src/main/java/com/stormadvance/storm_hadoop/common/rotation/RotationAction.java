package com.stormadvance.storm_hadoop.common.rotation;

import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public interface RotationAction extends Serializable {
	void execute(FileSystem fileSystem, Path filePath) throws IOException;
}
