package org.datavaultplatform.worker.operations;

import java.io.File;
import java.nio.file.Path;

public interface IPackager {

	boolean createBag(Path bagDirectory, Path dataDirectory);
	
	boolean validateBag(File dir);
	
	boolean addMetadata(File bagDir, String depositMetadata, String vaultMetadata, String fileTypeMetadata,
			String externalMetadata);
	
	boolean extractMetadata(File bagDir, File metaDir);
}
