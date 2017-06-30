package org.datavaultplatform.worker.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class FileWriterUtil {

	public static void writeToFile(File file, String data) {

		try {
			// FileUtils.writeStringToFile(manifest, data);
			FileUtils.writeStringToFile(file, data, true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
