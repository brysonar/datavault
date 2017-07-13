package org.datavaultplatform.common.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.GeneralSecurityException;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import org.datavaultplatform.common.exception.DataVaultException;

import oracle.cloudstorage.api.cipher.CryptoUtils;
import oracle.cloudstorage.api.cipher.CryptoUtils.DigestInputStream;


public class EncryptionOracleUtil {
	
    private static final String ALGORITHM = "AES";
    private static final String  HMAC = "HmacSHA256";
	private static final String key = "R0b1nTayl0r2017s";
	
	public static String doOracleEncrypt(File inputFile, File outputFile) {

		try (InputStream inputStream = Files.newInputStream(inputFile.toPath());
				FileOutputStream out = new FileOutputStream(outputFile)) {

//			KeyGenerator keyGen = javax.crypto.KeyGenerator.getInstance(ALGORITHM);
////			keyGen.init(128);
//			SecretKey secretKey = keyGen.generateKey();
			
			SecretKey secretKey = new SecretKeySpec(key.getBytes(), ALGORITHM);
			
			//SecretKey secretKey = CryptoUtils.createAESKey();
//			System.err.println("secretKey: " + secretKey.getAlgorithm());
//			System.err.println("secretKey: " + secretKey.getFormat());
//			System.err.println("secretKey: " + secretKey.getEncoded());
			
//			SecretKey hmac = CryptoUtils.createHMACKey();
			SecretKey hmac = new SecretKeySpec(key.getBytes(), HMAC);
			
//			System.err.println("hmac: " + hmac.getAlgorithm());
//			System.err.println("getFormat: " + hmac.getFormat());
//			System.err.println("getEncoded: " + new String(secretKey.getEncoded()));


			try (DigestInputStream digestInputStream = CryptoUtils
					.createEncryptionFilter(inputStream, secretKey, hmac)) {

				byte[] buffer = new byte[8192];

				int count;
				while ((count = digestInputStream.read(buffer)) > 0) {
					out.write(buffer, 0, count);
				}
				
				out.flush();
				
//				System.err.println("xxx: " + new String(digestInputStream.getDigest()));
//				Path decryptedPath = Paths.get("/dmp/stuff/db1dbbb8-6399-43fd-b7cb-4971ebfbbde3_b.tar");
//				doOracleDecrypt(outputFile, decryptedPath.toFile(), digestInputStream.getDigest());
				
				//System.err.println("length: " + digestInputStream.getDigest().length);
				byte[] digest = digestInputStream.getDigest();
//				String digestAsString = DatatypeConverter.printHexBinary(digest);
				String digestAsString = new HexBinaryAdapter().marshal(digest);			
				return digestAsString;
			}

		} catch (IOException | GeneralSecurityException e) {
			throw new DataVaultException("Error encrypting/decrypting file", e);
		}
	}
    
    
	public static void doOracleDecrypt(File inputFile, File outputFile, String digestAsString) {

		//String digestAsString = DatatypeConverter.print
		byte[] digest = new HexBinaryAdapter().unmarshal(digestAsString);
		
		try (InputStream inputStream = Files.newInputStream(inputFile.toPath());
				FileOutputStream out = new FileOutputStream(outputFile)) {

			SecretKey secretKey = new SecretKeySpec(key.getBytes(), ALGORITHM);
			SecretKey hmac = new SecretKeySpec(key.getBytes(), HMAC);
			
//			KeyGenerator k = javax.crypto.KeyGenerator.getInstance(ALGORITHM);
//			SecretKey secretKey = k.generateKey();
//			System.err.println("secretKeyxx: " + secretKey.getEncoded());
			
//			SecretKey secretKey = CryptoUtils.createAESKey();
//			System.err.println("secretKey: " + secretKey.getAlgorithm());
//			SecretKey hmac = CryptoUtils.createHMACKey();
//			System.err.println("hmac: " + hmac.getAlgorithm());

			//8192
			byte[] buffer = new byte[1024];

			try (InputStream decryptedInputStream = CryptoUtils
					.createDecryptionFilter(inputStream, secretKey, hmac, digest)) {

//				byte[] buffer = new byte[1024];
//				int decryptedLen = 0;
//				int i;
//				while ((i = decryptedInputStream.read(buffer, decryptedLen, buffer.length - decryptedLen)) > -1) {
//					decryptedLen += i;
//				}

				//decryptedInputStream.read(buffer, 0, 10)
				int count;
				while ((count = decryptedInputStream.read(buffer)) > 0) {
					out.write(buffer, 0, count);
				}
				out.flush();
			}

		} catch (IOException | GeneralSecurityException e) {
			throw new DataVaultException("Error encrypting/decrypting file", e);
		}
	}

}
