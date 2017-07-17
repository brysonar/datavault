package org.datavaultplatform.common.util;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.datavaultplatform.common.exception.DataVaultException;
import org.datavaultplatform.common.io.Progress;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public class FileUtilTest {

	private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);
	
	final Progress progress = null;
	
	final Path source = Paths.get("/datavaultdump/myfiles");
	final Path target = Paths.get("/datavaultdump2/myfiles2");
	
	final Path sourceFile = Paths.get("/datavaultdump/myfiles/red.txt");
	final Path targetFile = Paths.get("/datavaultdump2/myfiles3/red2.txt");
	

	@Test
	public void testDirectoryCopy() {

		try {
			FileUtil.copyDirectoryOrFile(progress, source, target);
		} catch (DataVaultException e) {
			logger.error(e.getMessage(), e);
			throw e;
		}
	}

	@Test
	public void testFileCopy() {

		try {
			FileUtil.copyDirectoryOrFile(progress, sourceFile, targetFile);
		} catch (DataVaultException e) {
			logger.error(e.getMessage(), e);
			throw e;
		}
	}
	
}
