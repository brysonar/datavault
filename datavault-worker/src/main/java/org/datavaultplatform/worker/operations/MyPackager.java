package org.datavaultplatform.worker.operations;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.datavaultplatform.common.storage.CheckSumEnum;
import org.datavaultplatform.worker.model.FileDetails;
import org.datavaultplatform.worker.util.CheckSumUtil;
import org.datavaultplatform.worker.util.DataVaultConstants;
import org.datavaultplatform.worker.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class MyPackager implements IPackager {

	private static final Logger logger = LoggerFactory.getLogger(MyPackager.class);

	private static final String metadataDirName = "metadata";

	private static final String depositMetaFileName = "deposit.json";
	private static final String vaultMetaFileName = "vault.json";
	private static final String fileTypeMetaFileName = "filetype.json";
	private static final String externalMetaFileName = "external.txt";

	@Autowired
	private ManifestGenerator manifestGenerator;

	@Override
	public boolean createBag(Path bagDirectory, Path dataDirectory) {

		logger.debug("Creating Bag: {}, {}", bagDirectory, dataDirectory);

		String manifestChecksum = manifestGenerator.generate(bagDirectory, dataDirectory);
		File tagManifestFile = bagDirectory.resolve(DataVaultConstants.TAG_MANIFEST_FILE).toFile();
		FileUtil.writeToFile(tagManifestFile, manifestChecksum + "," + DataVaultConstants.MANIFEST_FILE_NAME + ","
				+ getCheckSumEnum() + DataVaultConstants.NEW_LINE);

		// this return is simply due to the Bagit Packager having a return boolean
		// although the return value from bagit packager is not used
		return true;
	}

	// Validate an existing bag
	@Override
	public boolean validateBag(Path bagDirectory) {

		logger.debug("Validate Bag - {}", bagDirectory);
		// load manifest file
		Path manifestPath = bagDirectory.resolve(DataVaultConstants.MANIFEST_FILE_NAME);
		Path tempBagDataPath = bagDirectory.resolve(DataVaultConstants.DATA);
		logger.debug("tempBagDataPath - {}", tempBagDataPath);
		// validate checksum of each file
		listFiles(bagDirectory, tempBagDataPath, manifestPath);
		return true;
	}

	private void listFiles(final Path bagDirectory, final Path dataDirectory, final Path manifestFile) {
		
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDirectory)) {
			for (Path file : stream) {
				if (Files.isDirectory(file)) {
					listFiles(bagDirectory, file, manifestFile);
				} else {
					FileDetails fileDetails = new FileDetails(file, CheckSumUtil.generateCheckSum(file, getCheckSumEnum()));
					//verify checksum
					validateCheckSum(bagDirectory, manifestFile, fileDetails);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void validateCheckSum(final Path bagDirectory, final Path manifestFile, final FileDetails fileDetails) {
		logger.debug("validateCheckSum");

		boolean fileFound = false;

		try (BufferedReader reader = Files.newBufferedReader(manifestFile, Charset.forName("UTF-8"))) {

			String currentLine = null;

			while ((currentLine = reader.readLine()) != null) {

				String[] fields = currentLine.split(",");

				if (fields[1].equals(fileDetails.getFilePath(bagDirectory))) {
					fileFound = true;
					if (!fields[0].equals(fileDetails.getCheckSum())) {
						throw new RuntimeException("Checksum for " + fileDetails.getFilePath(bagDirectory)
								+ " does not match actual: " + fileDetails.getCheckSum() + " != expected: " +  fields[0]);
					} else {
						logger.trace("Checksum for " + fileDetails.getFilePath(bagDirectory) + " matches");
					}
				}

			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		if (!fileFound) {
			throw new RuntimeException("Could not find meta data checkum for " + fileDetails.getFilePath(bagDirectory));

		}

	}

	// this seems to keep a lock and therefore does not clean up delete
	private void validateCheckSum2(final Path bagDirectory, final Path manifestFile, final FileDetails fileDetails) {
		logger.debug("validateCheckSum");

		boolean fileFound = false;

		try {
			
		//	Files.lines(manifestFile).
			Iterator<String> i = Files.lines(manifestFile).iterator();
			while (i.hasNext()) {

				String[] fields = i.next().split(",");

				if (fields[1].equals(fileDetails.getFilePath(bagDirectory))) {
					fileFound = true;
					if (!fields[0].equals(fileDetails.getCheckSum())) {
						throw new RuntimeException("Checksum for " + fileDetails.getFilePath(bagDirectory)
								+ " does not match " + fields[0] + " != " + fileDetails.getCheckSum());
					} else {
						logger.trace("Checksum for " + fileDetails.getFilePath(bagDirectory) + " matches");
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (!fileFound) {
			throw new RuntimeException("Could not find meta data checkum for " + fileDetails.getFilePath(bagDirectory));

		}

	}
	
	private void validateCheckSum3(final Path bagDirectory, final Path manifestFile, final FileDetails fileDetails) {
		logger.debug("validateCheckSum");

		try {
			Files.lines(manifestFile).forEach(line -> {

				String[] fields = line.split(",");

				if (fields[1].equals(fileDetails.getFilePath(bagDirectory))) {
					logger.debug("matches yeah");
					if (!fields[0].equals(fileDetails.getCheckSum())) {
						throw new RuntimeException("Checksum for " + fileDetails.getFilePath(bagDirectory)
								+ " does not match " + fields[0] + " != " + fileDetails.getCheckSum());
					} else {
						logger.trace("Checksum for " + fileDetails.getFilePath(bagDirectory) + " matches");
					}
				}
			});// print each line

		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}
	
	// Add vault/deposit metadata
	@Override
	public boolean addMetadata(File bagDir, String depositMetadata, String vaultMetadata, String fileTypeMetadata,
			String externalMetadata) {

		logger.debug(
				"Adding Meta Data - bagDir: {}, depositMetadata: {}, vaultMetadata: {}, fileTypeMetadata: {}, externalMetadata: {}",
				bagDir.getAbsolutePath(), depositMetadata, vaultMetadata, fileTypeMetadata, externalMetadata);

		boolean result = false;

		try {
			Path bagPath = bagDir.toPath();

			// Create an empty "metadata" directory
			Path metadataDirPath = bagPath.resolve(metadataDirName);
			File metadataDir = metadataDirPath.toFile();
			metadataDir.mkdir();

			File tagManifestFile = bagPath.resolve(DataVaultConstants.TAG_MANIFEST_FILE).toFile();

			// Create metadata files and compute/store hashes
			addMetaFile(tagManifestFile, metadataDirPath, depositMetaFileName, depositMetadata, getCheckSumEnum());
			addMetaFile(tagManifestFile, metadataDirPath, vaultMetaFileName, vaultMetadata, getCheckSumEnum());
			addMetaFile(tagManifestFile, metadataDirPath, fileTypeMetaFileName, fileTypeMetadata, getCheckSumEnum());
			addMetaFile(tagManifestFile, metadataDirPath, externalMetaFileName, externalMetadata, getCheckSumEnum());

			// Metadata files created
			result = true;

		} catch (IOException e) {
			System.out.println(e.toString());
			result = false;
		}

		return result;

	}

	// Add a metadata file to the bag metadata directory
	// Also adds tag information to the tag manifest
	private void addMetaFile(File tagManifest, Path metadataDirPath, String metadataFileName, String metadata,
			CheckSumEnum checkSumEnum) throws IOException {

		logger.debug(
				"Adding Meta File - tagManifest: {}, metadataDirPath: {}, metadataFileName: {}, metadata: {}, alg: {}",
				tagManifest.getAbsolutePath(), metadataDirPath, metadataFileName, metadata, checkSumEnum);

		Path metadataFile = metadataDirPath.resolve(metadataFileName);
		FileUtils.writeStringToFile(metadataFile.toFile(), metadata);
		String hash = CheckSumUtil.generateCheckSum(metadataFile, checkSumEnum);
		FileUtils.writeStringToFile(tagManifest, hash + "," + metadataDirName + "/" + metadataFileName + ","
				+ checkSumEnum + DataVaultConstants.NEW_LINE, true);
	}

	// Extract the top-level metadata files from a bag and copy to a new
	// directory.
	@Override
	public boolean extractMetadata(File bagDir, File metaDir) {

		// TODO: could we use the built-in "holey" bag methods instead?

		boolean result = false;
		Path bagPath = bagDir.toPath();

		try (DirectoryStream<Path> stream = Files.newDirectoryStream(bagPath)) {
			for (Path entry : stream) {

				/*
				 * Expected: - data (dir) - bag-info.txt - bagit.txt -
				 * manifest-md5.txt - tagmanifest-md5.txt - other metadata files
				 * or directories
				 */

				String entryFileName = entry.getFileName().toString();

				if (Files.isDirectory(entry)) {
					// Handle directories
					if (entryFileName.equals("data")) {
						// Create an empty "data" directory
						Path metaDirPath = Paths.get(metaDir.toURI());
						File emptyDataDir = metaDirPath.resolve("data").toFile();
						emptyDataDir.mkdir();
					} else {
						FileUtils.copyDirectoryToDirectory(entry.toFile(), metaDir);
					}

				} else if (!Files.isDirectory(entry)) {
					// Handle files
					FileUtils.copyFileToDirectory(entry.toFile(), metaDir);
				}
			}

			// All files copied
			result = true;

		} catch (IOException e) {
			System.out.println(e.toString());
			result = false;
		}

		return result;
	}

	@Override
	public void addTarfileChecksum(Path bagDirectory, Path tarfile, String tarHash, CheckSumEnum tarHashAlgorithm) {

		File tagManifestFile = bagDirectory.resolve(DataVaultConstants.TAG_MANIFEST_FILE).toFile();
		FileUtil.writeToFile(tagManifestFile, tarHash + "," + tarfile.getFileName() + ","
				+ tarHashAlgorithm.getJavaSecurityAlgorithm() + DataVaultConstants.NEW_LINE);
	}

	private CheckSumEnum getCheckSumEnum() {
		return DataVaultConstants.CHECKSUM_ENUM;
	}
}
