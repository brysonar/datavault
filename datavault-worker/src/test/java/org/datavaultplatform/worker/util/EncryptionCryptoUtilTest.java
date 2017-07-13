package org.datavaultplatform.worker.util;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class EncryptionCryptoUtilTest {


	@Test
	public void testdDoOracleEncrypt() {
	
		Path inputFile = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3.tar");
		Path outputPath = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3_a.tar");
		Path decryptedPath = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3_b.tar");
		Assert.assertTrue(inputFile.toFile().exists());

		String digest = EncryptionOracleUtil.doOracleEncrypt(inputFile.toFile(), outputPath.toFile());
		EncryptionOracleUtil.doOracleDecrypt(outputPath.toFile(), decryptedPath.toFile(), digest);
		TarUtil.unTar(decryptedPath.toFile(), Paths.get("/dmp/stuff"));
	}
	
	
	@Test
	public void testEncryptDecrypt() {
	
		Path inputFile = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3.tar");
		Path outputPath = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3_a.tar");
		Path decryptedPath = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3_b.tar");
		Assert.assertTrue(inputFile.toFile().exists());

		EncryptionCryptoUtil.encrypt(inputFile.toFile(), outputPath.toFile());
		EncryptionCryptoUtil.decrypt(outputPath.toFile(), decryptedPath.toFile());
		TarUtil.unTar(decryptedPath.toFile(), Paths.get("/dmp/stuff"));
	}

	
	@Test
	public void testEncryptDecryptString() throws IOException {
		
		boolean do64Encoding = false;
		runEncoding(do64Encoding);
	}

	@Test
	public void testEncryptDecryptStringWith64Encoding() throws IOException {
		
		boolean do64Encoding = true;
		runEncoding(do64Encoding);
	}
	
	private void runEncoding(boolean do64Encoding) {
		String input = "hello world!";

		byte[] encryptedBytes = EncryptionCryptoUtil.encryptString(input, do64Encoding);
		//System.out.println("Encrypted: " + Arrays.toString(encryptedBytes));
		String value = EncryptionCryptoUtil.decryptToString(encryptedBytes, do64Encoding);
		//System.out.println("Decrypted: " + value);
		Assert.assertEquals(input, value);
	}
	
}
