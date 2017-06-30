package org.datavaultplatform.worker.operations;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.datavaultplatform.worker.util.ChecksumUtil;
import org.datavaultplatform.worker.util.DataVaultConstants;
import org.datavaultplatform.worker.util.FileWriterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class MyPackager implements IPackager {

	private static final String NEW_LINE = "\n";

	private static final Logger logger = LoggerFactory.getLogger(MyPackager.class);

	private static final String metadataDirName = "metadata";

	private static final String depositMetaFileName = "deposit.json";
	private static final String vaultMetaFileName = "vault.json";
	private static final String fileTypeMetaFileName = "filetype.json";
	private static final String externalMetaFileName = "external.txt";

	private final CheckSumEnum checkSumEnum = CheckSumEnum.MD5;

	@Autowired
	private ManifestGenerator manifestGenerator;
	
	@Override
	public boolean createBag(Path bagDirectory, Path dataDirectory) {

		logger.debug("Creating Bag: {}, {}", bagDirectory, dataDirectory);

		String manifestChecksum = manifestGenerator.generate(bagDirectory, dataDirectory);
		File tagManifestFile = bagDirectory.resolve(DataVaultConstants.TAG_MANIFEST_FILE).toFile();
		FileWriterUtil.writeToFile(tagManifestFile, manifestChecksum + "," + DataVaultConstants.MANIFEST_FILE_NAME + "," + checkSumEnum + NEW_LINE);

		//this return is simply due to the Bagit Packager having a return boolean 
		//although the return value from bagit packager is not used
		return true;
	}


	// Validate an existing bag
	@Override
	public boolean validateBag(File dir) {

		return true;
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
			addMetaFile(tagManifestFile, metadataDirPath, depositMetaFileName, depositMetadata, checkSumEnum);
			addMetaFile(tagManifestFile, metadataDirPath, vaultMetaFileName, vaultMetadata, checkSumEnum);
			addMetaFile(tagManifestFile, metadataDirPath, fileTypeMetaFileName, fileTypeMetadata, checkSumEnum);
			addMetaFile(tagManifestFile, metadataDirPath, externalMetaFileName, externalMetadata, checkSumEnum);

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
		String hash = ChecksumUtil.generateCheckSum(metadataFile, checkSumEnum);
		FileUtils.writeStringToFile(tagManifest, hash + "," + metadataDirName + "/" + metadataFileName  + "," + checkSumEnum + NEW_LINE, true);
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
}
