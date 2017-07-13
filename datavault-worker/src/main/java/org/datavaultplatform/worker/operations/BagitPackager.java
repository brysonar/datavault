package org.datavaultplatform.worker.operations;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.datavaultplatform.common.storage.CheckSumEnum;
import org.datavaultplatform.worker.util.CheckSumUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.loc.repository.bagit.Bag;
import gov.loc.repository.bagit.BagFactory;
import gov.loc.repository.bagit.Manifest.Algorithm;
import gov.loc.repository.bagit.PreBag;

public class BagitPackager implements IPackager {

	private static final Logger logger = LoggerFactory.getLogger(BagitPackager.class);
	
    public static final String metadataDirName = "metadata";
    
    public static final String depositMetaFileName = "deposit.json";
    public static final String vaultMetaFileName = "vault.json";
    public static final String fileTypeMetaFileName = "filetype.json";
    public static final String externalMetaFileName = "external.txt";
    
    // Create a bag from an existing directory.
    public boolean createBag(Path bagDirectory, Path dataDirectory)  {

    	logger.info("Creating bag ...");
    	
        BagFactory bagFactory = new BagFactory();
        PreBag preBag = bagFactory.createPreBag(bagDirectory.toFile());
        Bag bag = preBag.makeBagInPlace(BagFactory.LATEST, false);
        
        boolean result = false;
        try {
            result = bag.verifyValid().isSuccess();
        } finally {
            try {
				bag.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
        }
        
        return result;
    }
    
    // Validate an existing bag
    @Override
    public  boolean validateBag(Path bagDirectory) {

        BagFactory bagFactory = new BagFactory();
        Bag bag = bagFactory.createBag(bagDirectory.toFile());
        
        boolean result = false;
        try {
            result = bag.verifyValid().isSuccess();
        } finally {
            try {
				bag.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
        }
        
        return result;
    }
    
    // Add vault/deposit metadata
    @Override
    public boolean addMetadata(File bagDir,
                                      String depositMetadata,
                                      String vaultMetadata,
                                      String fileTypeMetadata,
                                      String externalMetadata) {
        
        boolean result = false;
        
        try {
            Path bagPath = bagDir.toPath();

            // Create an empty "metadata" directory
            Path metadataDirPath = bagPath.resolve(metadataDirName);
            File metadataDir = metadataDirPath.toFile();
            metadataDir.mkdir();
            
            // TODO: get the manifest file and algorithm config via bagit library?
            File tagManifest = bagPath.resolve("tagmanifest-md5.txt").toFile();
            Algorithm alg = Algorithm.MD5;
            
            // Create metadata files and compute/store hashes
            addMetaFile(tagManifest, metadataDirPath, depositMetaFileName, depositMetadata, alg);
            addMetaFile(tagManifest, metadataDirPath, vaultMetaFileName, vaultMetadata, alg);
            addMetaFile(tagManifest, metadataDirPath, fileTypeMetaFileName, fileTypeMetadata, alg);
            addMetaFile(tagManifest, metadataDirPath, externalMetaFileName, externalMetadata, alg);
            
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
    private void addMetaFile(File tagManifest, Path metadataDirPath, String metadataFileName, String metadata, Algorithm alg) throws IOException {
        
        File metadataFile = metadataDirPath.resolve(metadataFileName).toFile();
        FileUtils.writeStringToFile(metadataFile, metadata);
        String hash = CheckSumUtil.generateCheckSum(metadataFile, alg);
        FileUtils.writeStringToFile(tagManifest, hash + "  " + metadataDirName + "/" + metadataFileName + "\r\n", true);
    }
    
    
    // Extract the top-level metadata files from a bag and copy to a new directory.
    @Override
    public boolean extractMetadata(File bagDir, File metaDir) {
        
        // TODO: could we use the built-in "holey" bag methods instead?
        
        boolean result = false;
        Path bagPath = bagDir.toPath();
        
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(bagPath)) {
            for (Path entry : stream) {
                
                /*
                Expected:
                - data (dir)
                - bag-info.txt
                - bagit.txt
                - manifest-md5.txt
                - tagmanifest-md5.txt
                - other metadata files or directories
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
		
	}
}
