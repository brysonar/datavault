package org.datavaultplatform.worker.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;

import javax.xml.bind.DatatypeConverter;

import org.datavaultplatform.worker.operations.CheckSumEnum;

import com.amazonaws.util.Md5Utils;

import gov.loc.repository.bagit.Manifest.Algorithm;

public final class ChecksumUtil {

	private ChecksumUtil() {
	}
	
	//CompleterHelper
//  Algorithm algorithm = Algorithm.MD5;
//	String checksum = MessageDigestHelper.generateFixity(bagFile.newInputStream(), algorithm);

	public static String generateCheckSum(Path path, CheckSumEnum alg) {
		return generateCheckSumAws(path, alg);
	}
	
	public static String generateCheckSum(File file, CheckSumEnum alg) {
		return generateCheckSum(file.toPath(), alg);
	}
	
	private static String generateCheckSum2(Path path, CheckSumEnum alg) {
		return generateCheckSum2(path.toFile(), alg);
	}
	
	private static String generateCheckSum2(File file, CheckSumEnum alg) {

		String hash = null;
		try (FileInputStream fis = new FileInputStream(file)) {

			if (alg == CheckSumEnum.MD5) {
				hash = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
			} else if (alg == CheckSumEnum.SHA1) {
				hash = org.apache.commons.codec.digest.DigestUtils.sha1Hex(fis);
			} else if (alg == CheckSumEnum.SHA256) {
				hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(fis);
			} else if (alg == CheckSumEnum.SHA512) {
				hash = org.apache.commons.codec.digest.DigestUtils.sha512Hex(fis);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return hash;
	}

	private static String generateCheckSumAws(Path path, CheckSumEnum alg) {
		String checksum = null;
		try {
			if (alg == CheckSumEnum.MD5) {
				// checksum = Md5Utils.md5AsBase64(file.path.toFile());
				checksum = DatatypeConverter.printHexBinary(Md5Utils.computeMD5Hash(path.toFile()));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return checksum;
	}
	
	private static String generateCheckSum3(Path path, CheckSumEnum alg) {
		String checksum = null;
		try {
			byte[] b = Files.readAllBytes(path);
			byte[] hash = MessageDigest.getInstance(alg.MD5.name()).digest(b);
			checksum = DatatypeConverter.printHexBinary(hash);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return checksum;
	}
	


	//fast checksum = check pom dependency to enable
	//String hash = MD5.asHex(MD5.getHash(file.path.toFile()));
	
	
    // Compute a hash value for file contents
	public static String computeFileHash(File file, Algorithm alg) throws FileNotFoundException, IOException {

    	String hash = null;
    	FileInputStream fis = new FileInputStream(file);

        if (alg == Algorithm.MD5) {
            hash = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
        } else if (alg == Algorithm.SHA1) {
            hash = org.apache.commons.codec.digest.DigestUtils.sha1Hex(fis);
        } else if (alg == Algorithm.SHA256) {
            hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(fis);
        } else if (alg == Algorithm.SHA512) {
            hash = org.apache.commons.codec.digest.DigestUtils.sha512Hex(fis);
        }
        
        fis.close();
        return hash;
    }
    

	
}
