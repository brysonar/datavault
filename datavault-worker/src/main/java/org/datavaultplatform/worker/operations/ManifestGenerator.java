package org.datavaultplatform.worker.operations;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.datavaultplatform.worker.model.FileDetails;
import org.datavaultplatform.worker.util.ChecksumUtil;
import org.datavaultplatform.worker.util.DataVaultConstants;
import org.datavaultplatform.worker.util.FileWriterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManifestGenerator {

	private static final Logger logger = LoggerFactory.getLogger(ManifestGenerator.class);

	private final CheckSumEnum checkSumEnum = CheckSumEnum.MD5;
	
	public String generate(Path bagDirectory, Path dataDirectory) {

		logger.debug("Creating Bag: {}, {}", bagDirectory, dataDirectory);

		Path manifestPath = writeManifestFile(bagDirectory, dataDirectory);
		String manifestChecksum = ChecksumUtil.generateCheckSum2(manifestPath, checkSumEnum);
		return manifestChecksum;
	}

	private Path writeManifestFile(Path bagDirectory, Path dataDirectory) {
		
		Path manifestPath = bagDirectory.resolve(DataVaultConstants.MANIFEST_FILE_NAME);
		File manifestFile = manifestPath.toFile();

		final List<FileDetails> fileDetailsList = new ArrayList<>();
		listFiles(dataDirectory, fileDetailsList);

		fileDetailsList.stream().forEach(file -> {
			String line = file.getCheckSum() + "," + file.getFilePath(bagDirectory) + "," + checkSumEnum + DataVaultConstants.NEW_LINE;
			FileWriterUtil.writeToFile(manifestFile, line);
		});
		return manifestPath;
	}

	private void listFiles(Path path, List<FileDetails> fileDetailsList) {
		
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
			for (Path entry : stream) {
				if (Files.isDirectory(entry)) {
					listFiles(entry, fileDetailsList);
				} else {
					FileDetails fileDetails = new FileDetails(entry, ChecksumUtil.generateCheckSum2(entry, checkSumEnum));
					fileDetailsList.add(fileDetails);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
