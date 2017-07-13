package org.datavaultplatform.worker.util;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class EncryptionApacheUtilTest {

	@Test
	public void testEncryptDecrypt() throws IOException {
	
		Path inputFile = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3.tar");
		Path outputPath = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3_a.tar");
		Path decryptedPath = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3_b.tar");

		Assert.assertTrue(inputFile.toFile().exists());

		EncryptionApacheUtil.encrypt(inputFile.toFile(), outputPath.toFile());
		EncryptionApacheUtil.decrypt(outputPath.toFile(), decryptedPath.toFile());
		TarUtil.unTar(decryptedPath.toFile(), Paths.get("/dmp/stuff"));
	}
	
	@Ignore
	@Test
	public void test() throws Exception {
		
		Path inputFile = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3.tar");
		Path outputPath = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3_a.tar");
		
		//This method has not been completed
		EncryptionApacheUtil.encryptFile(inputFile.toFile(), outputPath.toFile());
	}
	
	@Test
	public void testEncryptDecryptString() throws IOException {
		
		String input = "hello world!";
		
		byte[] encryptedBytes = EncryptionApacheUtil.encryptString(input);
		//System.out.println("Encrypted: " + Arrays.toString(encryptedBytes));
		String value = EncryptionApacheUtil.decryptAsString(encryptedBytes);
		//System.out.println("Decrypted: " + value);
		Assert.assertEquals(input, value);
	}

}
