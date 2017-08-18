package com.stormadvance.storm_hadoop.common.rotation;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoveFileAction implements RotationAction {
	private static final Logger LOG = LoggerFactory
			.getLogger(MoveFileAction.class);

	private String destination;

	public MoveFileAction toDestination(String destDir) {
		destination = destDir;
		return this;
	}

	public void execute(FileSystem fileSystem, Path filePath)
			throws IOException {
		Path destPath = new Path(destination, filePath.getName());
		LOG.info("Moving file {} to {}", filePath, destPath);
		boolean success = fileSystem.rename(filePath, destPath);
		return;
	}
}
