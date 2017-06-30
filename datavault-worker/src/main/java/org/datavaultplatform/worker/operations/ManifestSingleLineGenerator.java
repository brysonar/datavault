package org.datavaultplatform.worker.operations;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.datavaultplatform.worker.model.FileDetails;
import org.datavaultplatform.worker.util.ChecksumUtil;
import org.datavaultplatform.worker.util.DataVaultConstants;
import org.datavaultplatform.worker.util.FileWriterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManifestSingleLineGenerator implements ManifestGenerator {

	private static final Logger logger = LoggerFactory.getLogger(ManifestSingleLineGenerator.class);

	@Override
	public String generate(Path bagDirectory, Path dataDirectory) {

		logger.debug("Creating Bag: {}, {}", bagDirectory, dataDirectory);

		Path manifestPath = writeManifestFile(bagDirectory, dataDirectory);
		String manifestChecksum = ChecksumUtil.generateCheckSum(manifestPath, getCheckSumEnum());
		return manifestChecksum;
	}

	private Path writeManifestFile(Path bagDirectory, Path dataDirectory) {
		
		Path manifestPath = bagDirectory.resolve(DataVaultConstants.MANIFEST_FILE_NAME);
		File manifestFile = manifestPath.toFile();
		listFiles(dataDirectory, bagDirectory, manifestFile);
		return manifestPath;
	}

	private void listFiles(final Path path, final Path bagDirectory, File manifestFile) {
		
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
			for (Path file : stream) {
				if (Files.isDirectory(file)) {
					listFiles(file, bagDirectory, manifestFile);
				} else {
					FileDetails fileDetails = new FileDetails(file, ChecksumUtil.generateCheckSum(file, getCheckSumEnum()));
					String line = fileDetails.getCheckSum() + "," + fileDetails.getFilePath(bagDirectory) + "," + getCheckSumEnum() + DataVaultConstants.NEW_LINE;
					FileWriterUtil.writeToFile(manifestFile, line);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private CheckSumEnum getCheckSumEnum() {
		return DataVaultConstants.checkSumEnum;
	}
}
