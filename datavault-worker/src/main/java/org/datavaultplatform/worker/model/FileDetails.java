package org.datavaultplatform.worker.model;

import java.nio.file.Path;

import gov.loc.repository.bagit.utilities.FilenameHelper;

public class FileDetails {
	
	private final Path path;
	private final String checkSum;
	
	public FileDetails(Path path, String checkSum) {
		super();
		this.path = path;
		this.checkSum = checkSum;
	}
	
	public String getFilePath(Path bagPath) {
		String filepath = FilenameHelper.removeBasePath(bagPath.toString(), path.toString());
		return filepath;
	}

	public Path getPath() {
		return path;
	}

	public String getCheckSum() {
		return checkSum;
	}

}

