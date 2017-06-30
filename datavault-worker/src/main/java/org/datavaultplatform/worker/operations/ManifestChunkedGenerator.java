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

public class ManifestChunkedGenerator implements ManifestGenerator {

	private static final int FILE_LIST_CHUNK_SIZE = 1;
	private static final Logger logger = LoggerFactory.getLogger(ManifestChunkedGenerator.class);

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

		final List<FileDetails> fileDetailsList = new ArrayList<>();
		listFiles(dataDirectory, fileDetailsList, bagDirectory, manifestFile);
		return manifestPath;
	}



	private void listFiles(Path path, List<FileDetails> fileDetailsList, Path bagDirectory, File manifestFile) {
		
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
			for (Path entry : stream) {
				if (Files.isDirectory(entry)) {
					listFiles(entry, fileDetailsList, bagDirectory, manifestFile);
				} else {
					FileDetails fileDetails = new FileDetails(entry, ChecksumUtil.generateCheckSum(entry, getCheckSumEnum()));
					fileDetailsList.add(fileDetails);
				}
				
				if (fileDetailsList.size() == FILE_LIST_CHUNK_SIZE) {
					writeListToFile(bagDirectory, manifestFile, fileDetailsList);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		
	}

	private void writeListToFile(Path bagDirectory, File manifestFile, final List<FileDetails> fileDetailsList) {
		fileDetailsList.stream().forEach(file -> {
			String line = file.getCheckSum() + "," + file.getFilePath(bagDirectory) + "," + getCheckSumEnum() + DataVaultConstants.NEW_LINE;
			FileWriterUtil.writeToFile(manifestFile, line);
		});
	}
	
	private CheckSumEnum getCheckSumEnum() {
		return DataVaultConstants.checkSumEnum;
	}
}
