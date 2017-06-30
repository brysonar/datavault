package org.datavaultplatform.worker;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.datavaultplatform.worker.operations.Tar;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TarTest {

	@Test
	public void test() throws Exception {
		
		//Tar.createTar(bagDir, tarFile);

		Path path = Paths.get("/dmp/" + "db1dbbb8-6399-43fd-b7cb-4971ebfbbde3.tar");
		
		System.err.println("exists: " + path.toFile().exists());
		System.err.println("Is File: " + path.toFile().isFile());
		
		Tar.unTar(path.toFile(), Paths.get("/dmp/output"));
		
	}
}
