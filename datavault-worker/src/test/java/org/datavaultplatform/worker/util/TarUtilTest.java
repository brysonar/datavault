package org.datavaultplatform.worker.util;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TarUtilTest {

	@Test
	public void testTar() throws Exception {
		
		Path path = Paths.get("/dmp/output/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3");
		
		System.err.println("exists: " + path.toFile().exists());
		System.err.println("Is File: " + path.toFile().isFile());
		System.err.println("Is Directory: " + path.toFile().isDirectory());
		
		Path outputPath = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3.tar");

		TarUtil.createTar(path.toFile(), outputPath.toFile());
		
	}
	
	@Test
	public void testUnTar() throws Exception {

		Path path = Paths.get("/dmp/stuff/" + "db1dbbb8-6399-43fd-b7cb-4971ebfbbde3_a.tar");
		
		System.err.println("exists: " + path.toFile().exists());
		System.err.println("Is File: " + path.toFile().isFile());
		
		TarUtil.unTar(path.toFile(), Paths.get("/dmp/output"));
		
	}
}
