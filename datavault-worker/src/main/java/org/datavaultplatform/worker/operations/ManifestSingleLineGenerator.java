package org.datavaultplatform.worker.operations;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.datavaultplatform.common.storage.CheckSumEnum;
import org.datavaultplatform.common.util.DataVaultConstants;
import org.datavaultplatform.worker.model.FileDetails;
import org.datavaultplatform.worker.util.CheckSumUtil;
import org.datavaultplatform.worker.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManifestSingleLineGenerator implements ManifestGenerator {

	private static final Logger logger = LoggerFactory.getLogger(ManifestSingleLineGenerator.class);

	@Override
	public String generate(Path bagDirectory, Path dataDirectory) {

		logger.debug("Creating Bag: {}, {}", bagDirectory, dataDirectory);

		Path manifestPath = writeManifestFile(bagDirectory, dataDirectory);
		String manifestChecksum = CheckSumUtil.generateCheckSum(manifestPath, getCheckSumEnum());
		return manifestChecksum;
	}

	private Path writeManifestFile(Path bagDirectory, Path dataDirectory) {
		
		Path manifestPath = bagDirectory.resolve(DataVaultConstants.MANIFEST_FILE_NAME);
		File manifestFile = manifestPath.toFile();
		listFiles(dataDirectory, bagDirectory, manifestFile);
		return manifestPath;
	}

	private void listFiles(final Path dataDirectory, final Path bagDirectory, File manifestFile) {
		
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDirectory)) {
			for (Path file : stream) {
				if (Files.isDirectory(file)) {
					listFiles(file, bagDirectory, manifestFile);
				} else {
					FileDetails fileDetails = new FileDetails(file, CheckSumUtil.generateCheckSum(file, getCheckSumEnum()));
					String line = fileDetails.getCheckSum() + "," + fileDetails.getFilePath(bagDirectory) + "," + getCheckSumEnum() + DataVaultConstants.NEW_LINE;
					FileUtil.writeToFile(manifestFile, line);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private CheckSumEnum getCheckSumEnum() {
		return DataVaultConstants.CHECKSUM_ENUM;
	}
}
