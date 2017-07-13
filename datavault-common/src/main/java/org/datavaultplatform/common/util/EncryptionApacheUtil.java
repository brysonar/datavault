package org.datavaultplatform.common.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.crypto.cipher.CryptoCipher;
import org.apache.commons.crypto.stream.CryptoInputStream;
import org.apache.commons.crypto.stream.CryptoOutputStream;
import org.apache.commons.crypto.utils.Utils;

public class EncryptionApacheUtil {

//https://commons.apache.org/proper/commons-crypto/userguide.html
	
	private static final String ALGORITHM = "AES";
	private static final String transform = "AES/CBC/PKCS5Padding";
	private static final String key = "1234567890123456";

	public static void encrypt(File inputFile, File outputFile) throws IOException {

		byte[] keyAsBytes = getUTF8Bytes(key);

		final SecretKeySpec key = new SecretKeySpec(keyAsBytes, ALGORITHM);
		final IvParameterSpec iv = new IvParameterSpec(keyAsBytes);

		Properties properties = new Properties();

		try (InputStream inputStream = Files.newInputStream(inputFile.toPath());
				OutputStream outputStream = Files.newOutputStream(outputFile.toPath());
				CryptoOutputStream out = new CryptoOutputStream(transform, properties, outputStream, key, iv)) {

			// 8192
			byte[] buffer = new byte[1024];
			int count;
			while ((count = inputStream.read(buffer)) > 0) {
				out.write(buffer, 0, count);
			}
			out.flush();
		}
	}
	
	public static void decrypt(File inputFile, File outputFile) throws IOException {

		byte[] keyAsBytes = getUTF8Bytes(key);
		final SecretKeySpec key = new SecretKeySpec(keyAsBytes, ALGORITHM);
		final IvParameterSpec iv = new IvParameterSpec(keyAsBytes);
		Properties properties = new Properties();

		try (OutputStream out = Files.newOutputStream(outputFile.toPath());
				InputStream inputStream = Files.newInputStream(inputFile.toPath());
				CryptoInputStream cis = new CryptoInputStream(transform, properties, inputStream, key, iv)) {

			byte[] buffer = new byte[1024];
			int len = -1;
			while ((len = cis.read(buffer)) != -1) {
				out.write(buffer, 0, len);
			}
			
			out.flush();
		}
	}
	

	//DO not use - not completed
	public static void encryptFile(File inputFile, File outputFile) throws Exception {

		byte[] keyAsBytes = getUTF8Bytes(key);

		final SecretKeySpec key = new SecretKeySpec(keyAsBytes, ALGORITHM);
		final IvParameterSpec iv = new IvParameterSpec(keyAsBytes);

		Properties properties = new Properties();

		final ByteBuffer outBuffer;
		final int bufferSize = 1024;
		final int updateBytes;
		final int finalBytes;

		byte[] inputBytes = null;
		try (FileInputStream inputStream = new FileInputStream(inputFile)) {
			inputBytes = new byte[(int) inputFile.length()];
			inputStream.read(inputBytes);
		}

		try (CryptoCipher encipher = Utils.getCipherInstance(transform, properties)) {

			ByteBuffer inBuffer = ByteBuffer.allocateDirect(bufferSize);
			outBuffer = ByteBuffer.allocateDirect(bufferSize);
			inBuffer.put(inputBytes);

			inBuffer.flip(); // ready for the cipher to read it
			// // Show the data is there
			// System.out.println("inBuffer=" + asString(inBuffer));

			// Initializes the cipher with ENCRYPT_MODE,key and iv.
			encipher.init(Cipher.ENCRYPT_MODE, key, iv);
			// Continues a multiple-part encryption/decryption operation for
			// byte buffer.
			updateBytes = encipher.update(inBuffer, outBuffer);
			System.out.println(updateBytes);

			// We should call do final at the end of encryption/decryption.
			finalBytes = encipher.doFinal(inBuffer, outBuffer);
			// System.out.println(finalBytes);
		}

		// outBuffer

		// outBuffer.flip(); // ready for use as decrypt
		// byte [] encoded = new byte[updateBytes + finalBytes];
		// outBuffer.duplicate().get(encoded);
		// System.out.println(Arrays.toString(encoded));
		//
		// // Now reverse the process
		// try (CryptoCipher decipher = Utils.getCipherInstance(transform,
		// properties)) {
		// decipher.init(Cipher.DECRYPT_MODE, key, iv);
		// ByteBuffer decoded = ByteBuffer.allocateDirect(bufferSize);
		// decipher.update(outBuffer, decoded);
		// decipher.doFinal(outBuffer, decoded);
		// decoded.flip(); // ready for use
		// System.out.println("decoded="+asString(decoded));
		// }
	}
	   
	   
	private static String asString(ByteBuffer buffer) {
		final ByteBuffer copy = buffer.duplicate();
		final byte[] bytes = new byte[copy.remaining()];
		copy.get(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}
	   
	public static byte[] encryptString(String input) throws IOException {

		byte[] keyAsBytes = getUTF8Bytes(key);
		
		final SecretKeySpec key = new SecretKeySpec(keyAsBytes, ALGORITHM);
		final IvParameterSpec iv = new IvParameterSpec(keyAsBytes);

		Properties properties = new Properties();

		// Encryption with CryptoOutputStream.
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try (CryptoOutputStream cos = new CryptoOutputStream(transform, properties, outputStream, key, iv)) {
			cos.write(getUTF8Bytes(input));
			cos.flush();
		}

		// The encrypted data:
		return outputStream.toByteArray();

	}


	
	public static String decryptAsString(byte[] encryptedBytes) throws IOException {

		byte[] keyAsBytes = getUTF8Bytes(key);
		
		final SecretKeySpec key = new SecretKeySpec(keyAsBytes, ALGORITHM);
		final IvParameterSpec iv = new IvParameterSpec(keyAsBytes);
		Properties properties = new Properties();

		// Decryption with CryptoInputStream.
		InputStream inputStream = new ByteArrayInputStream(encryptedBytes);

		try (CryptoInputStream cis = new CryptoInputStream(transform, properties, inputStream, key, iv)) {
			byte[] decryptedData = new byte[1024];
			int decryptedLen = 0;
			int i;
			while ((i = cis.read(decryptedData, decryptedLen, decryptedData.length - decryptedLen)) > -1) {
				decryptedLen += i;
			}
			return new String(decryptedData, 0, decryptedLen, StandardCharsets.UTF_8);
		}

	}

	private static byte[] getUTF8Bytes(String input) {
		return input.getBytes(StandardCharsets.UTF_8);
	}

}
