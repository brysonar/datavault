package org.datavaultplatform.worker.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;

import javax.xml.bind.DatatypeConverter;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

import org.datavaultplatform.common.storage.CheckSumEnum;
import org.datavaultplatform.worker.exception.DataVaultWorkerException;

import com.amazonaws.util.Md5Utils;

import gov.loc.repository.bagit.Manifest.Algorithm;

public final class CheckSumUtil {

	private CheckSumUtil() {
	}
	
	//CompleterHelper
//  Algorithm algorithm = Algorithm.MD5;
//	String checksum = MessageDigestHelper.generateFixity(bagFile.newInputStream(), algorithm);

	public static String generateCheckSum(Path path, CheckSumEnum alg) {
		
		//return generateCheckSumAws(path, alg);
		return generateCheckSumDigest(path, alg);
//		return generateCheckSumMessageDisgest(path, alg);
	}
	
	public static String generateCheckSum(File file, CheckSumEnum alg) {
		return generateCheckSum(file.toPath(), alg);
	}
	
	private static String generateCheckSumDigest(Path path, CheckSumEnum alg) {
		return generateCheckSumDigest(path.toFile(), alg);
	}
	
	private static String generateCheckSumDigest(File file, CheckSumEnum alg) {

		String hash = null;
		try (FileInputStream fis = new FileInputStream(file)) {

			if ( CheckSumEnum.MD5.equals(alg)) {
				hash = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
			} else if (CheckSumEnum.SHA1.equals(alg)) {
				hash = org.apache.commons.codec.digest.DigestUtils.sha1Hex(fis);
			} else if (CheckSumEnum.SHA256.equals(alg)) {
				hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(fis);
			} else if (CheckSumEnum.SHA512.equals(alg)) {
				hash = org.apache.commons.codec.digest.DigestUtils.sha512Hex(fis);
			}
		} catch (IOException e) {
			throw new DataVaultWorkerException(e);
		}
		return hash;
	}

	public static String generateCheckSum(File file, Algorithm alg) throws FileNotFoundException, IOException {

		String hash = null;
		try (FileInputStream fis = new FileInputStream(file)) {

			if (Algorithm.MD5.equals(alg)) {
				hash = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
			} else if (Algorithm.SHA1.equals(alg)) {
				hash = org.apache.commons.codec.digest.DigestUtils.sha1Hex(fis);
			} else if (Algorithm.SHA256.equals(alg)) {
				hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(fis);
			} else if (Algorithm.SHA512.equals(alg)) {
				hash = org.apache.commons.codec.digest.DigestUtils.sha512Hex(fis);
			}
		}

		return hash;
	}
	
	private static String generateCheckSumAws(Path path, CheckSumEnum alg) {
		String checksum = null;
		try {
			if (CheckSumEnum.MD5.equals(alg)) {
				// checksum = Md5Utils.md5AsBase64(file.path.toFile());
				checksum = DatatypeConverter.printHexBinary(Md5Utils.computeMD5Hash(path.toFile()));
			} else {
				throw new DataVaultWorkerException("Checksum type " + alg.getJavaSecurityAlgorithm() + " not supported");
			}
		} catch (IOException e) {
			throw new DataVaultWorkerException(e);
		}
		return checksum;
	}
	
	private static String generateCheckSumMessageDisgest(Path path, CheckSumEnum alg) {

		try {
			MessageDigest md = MessageDigest.getInstance(alg.getJavaSecurityAlgorithm());
			byte[] b = Files.readAllBytes(path);
			return DatatypeConverter.printHexBinary(md.digest(b));

		} catch (Exception e) {
			throw new DataVaultWorkerException(e);
		}
	}

	public static String getDigest(File file, CheckSumEnum alg) {

		try {
			MessageDigest md = MessageDigest.getInstance(alg.getJavaSecurityAlgorithm());

			try (InputStream is = new FileInputStream(file)) {
				byte[] buffer = new byte[8192];
				int len = is.read(buffer);

				while (len != -1) {
					md.update(buffer, 0, len);
					len = is.read(buffer);
				}

				return new HexBinaryAdapter().marshal(md.digest());
			}
		} catch (Exception e) {
			throw new DataVaultWorkerException(e.getMessage(), e);
		}
	}
    
	//fast checksum = check pom dependency to enable
	//String hash = MD5.asHex(MD5.getHash(file.path.toFile()));
	
}
