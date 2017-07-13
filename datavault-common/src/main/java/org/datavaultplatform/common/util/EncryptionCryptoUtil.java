package org.datavaultplatform.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.datavaultplatform.common.exception.DataVaultException;

//http://www.codejava.net/coding/file-encryption-and-decryption-simple-example
public class EncryptionCryptoUtil {

	private static final String key = "R0b1nTayl0r2017s";
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES";

    public static void encrypt(File inputFile, File outputFile) {
    	
        doCrypto(Cipher.ENCRYPT_MODE, inputFile, outputFile);
    }
 
    public static void decrypt(File inputFile, File outputFile) {
        doCrypto(Cipher.DECRYPT_MODE, inputFile, outputFile);
    }

	private static void doCrypto(int cipherMode, File inputFile, File outputFile) {

		try {
			Key secretKey = new SecretKeySpec(key.getBytes(), ALGORITHM);

			Cipher cipher = Cipher.getInstance(TRANSFORMATION);
			cipher.init(cipherMode, secretKey);

			try (InputStream inputStream = Files.newInputStream(inputFile.toPath());
					CipherOutputStream out = new CipherOutputStream(new FileOutputStream(outputFile), cipher)) {

				byte[] buffer = new byte[8192];
				int count;
				while ((count = inputStream.read(buffer)) > 0) {
					out.write(buffer, 0, count);
				}
				out.flush();
			}

		} catch (NoSuchPaddingException | NoSuchAlgorithmException | InvalidKeyException | IOException ex) {
			throw new DataVaultException("Error encrypting/decrypting file", ex);
		}

	}
    
    //DO not use as it copies the whole file in to byte array
    private static void doCryptoWholeFile(int cipherMode, File inputFile,
            File outputFile) {

        try (InputStream inputStream = new FileInputStream(inputFile)) {
            
        	Key secretKey = new SecretKeySpec(key.getBytes(), ALGORITHM);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(cipherMode, secretKey);
             
            byte[] inputBytes = new byte[(int) inputFile.length()];
            inputStream.read(inputBytes);
             
//            byte[] toencrypt = Files.readAllBytes(path);         
            
            byte[] outputBytes = cipher.doFinal(inputBytes);
             
            try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
            	outputStream.write(outputBytes);
            }
             
        } catch (NoSuchPaddingException | NoSuchAlgorithmException
                | InvalidKeyException | BadPaddingException
                | IllegalBlockSizeException | IOException ex) {
            throw new DataVaultException("Error encrypting/decrypting file", ex);
        }
    }
    

	private static SecretKeySpec getSecretKeySpec(String myKey) {

		MessageDigest sha = null;
		try {
			byte[] key = myKey.getBytes("UTF-8");
			sha = MessageDigest.getInstance("SHA-1");
			key = sha.digest(key);
			key = Arrays.copyOf(key, 16);
			return new SecretKeySpec(key, "AES");
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new DataVaultException(e);
		} 
	}

	

	public static byte[] encryptString(String strToEncrypt, boolean do64Encoding) {
		try {
			//SecretKeySpec secretKey = getSecretKeySpec(key);
			Key secretKey = new SecretKeySpec(key.getBytes(), ALGORITHM);
			
			Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);

			byte[] bytes = cipher.doFinal(strToEncrypt.getBytes("UTF-8"));

			if (do64Encoding) {
				return Base64.getEncoder().encode(bytes);
			}
			return bytes;
		} catch (Exception e) {
			throw new DataVaultException("Error while encrypting: " + e.getMessage(), e);
		}
	}

	public static String decryptToString(byte[] input, boolean do64Encoding) {
		
		//SecretKeySpec secretKey = getSecretKeySpec(key);
		Key secretKey = new SecretKeySpec(key.getBytes(), ALGORITHM);
		return decryptToString(input, do64Encoding, secretKey);
	}

	public static String decryptToString(byte[] input, boolean do64Encoding, Key secretKey) {
		try {
			Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
			cipher.init(Cipher.DECRYPT_MODE, secretKey);

			byte[] bytes = null;

			if (do64Encoding) {
				bytes = cipher.doFinal(Base64.getDecoder().decode(input));
			} else {
				bytes = cipher.doFinal(input);
			}

			return new String(bytes);
		} catch (Exception e) {
			throw new DataVaultException("Error while decrypting: " + e.getMessage(), e);
		}
	}
}
