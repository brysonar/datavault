package org.datavaultplatform.worker.operations;

import java.io.File;
import java.nio.file.Path;

import org.datavaultplatform.common.storage.CheckSumEnum;

public interface IPackager {

	boolean createBag(Path bagDirectory, Path dataDirectory);
	
	boolean validateBag(Path bagDirectory);
	
	boolean addMetadata(File bagDir, String depositMetadata, String vaultMetadata, String fileTypeMetadata,
			String externalMetadata);
	
	void addTarfileChecksum(Path bagDirectory, Path tarfile,  String tarHash, CheckSumEnum tarHashAlgorithm);
	 
	boolean extractMetadata(File bagDir, File metaDir);
}
