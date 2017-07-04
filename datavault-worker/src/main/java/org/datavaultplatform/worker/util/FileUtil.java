package org.datavaultplatform.worker.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.datavaultplatform.worker.exception.DataVaultWorkerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FileUtil {

	private final static Logger logger = LoggerFactory.getLogger(FileUtil.class);

	private FileUtil() {
		super();
	}


	public static void writeToFile(File file, String data) {

		try {
			// FileUtils.writeStringToFile(manifest, data);
			FileUtils.writeStringToFile(file, data, true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	
	public static void deleteDirectory(Path path) {
		logger.debug("Deleting Directory: {}", path.toString());
		try {
			FileUtils.deleteDirectory(path.toFile());
		} catch (IOException e) {
			throw new DataVaultWorkerException("Failed to delete directory: " + path, e);
		}
	}
	
	public static void deleteDirectory(File dir) {
		// Cleanup
		logger.info("Deleting directory: {}", dir.getAbsolutePath());
		try {
			FileUtils.deleteDirectory(dir);
		} catch (IOException io) {
			throw new DataVaultWorkerException("Failed to delete directory: " + dir.getAbsolutePath());
		}
	}
	
	
	public static void createDirectory(Path path) {
		logger.debug("Creating Directory: {}", path.toString());
		File tempDirectory = path.toFile();
		boolean result = tempDirectory.mkdir();

		if (!result) {
			throw new DataVaultWorkerException("Failed to create directory: " + path);
		}
	}
	
}
