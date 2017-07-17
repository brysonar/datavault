package org.datavaultplatform.common.util;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.datavaultplatform.common.exception.DataVaultException;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class FileUtilTest {

	final Path source = Paths.get("/datavaultdump/myfiles");
	final Path target = Paths.get("/datavaultdump2/myfiles2");
	
	final Path sourceFile = Paths.get("/datavaultdump/myfiles/red.txt");
	final Path targetFile = Paths.get("/datavaultdump2/myfiles3/red2.txt");
	

	@Test
	public void testDirectoryCopy() {

		try {
			FileUtil.copyFile(source, target);
		} catch (DataVaultException e) {
			System.err.println(e.getMessage());
		}
	}

	@Test
	public void testFileCopy() {

		try {
			FileUtil.copyFile(sourceFile, targetFile);
		} catch (DataVaultException e) {
			System.err.println(e.getMessage());
		}
	}
	
}
