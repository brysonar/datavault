package org.datavaultplatform.worker.operations;

import gov.loc.repository.bagit.Manifest.Algorithm;

public enum CheckSumEnum {

		MD5 ("md5", "MD5"), SHA1 ("sha1", "SHA-1"), SHA256 ("sha256", "SHA-256"), SHA512 ("sha512", "SHA-512");
		
		public String bagItAlgorithm;
		public String javaSecurityAlgorithm;
		
		CheckSumEnum(String bagItAlgorithm, String javaSecurityAlgorithm) {
			this.bagItAlgorithm = bagItAlgorithm;
			this.javaSecurityAlgorithm = javaSecurityAlgorithm;
		}
		
		public static Algorithm valueOfBagItAlgorithm(String bagItAlgorithm) throws IllegalArgumentException {
			for(Algorithm algorithm : Algorithm.values()) {
				if (bagItAlgorithm.equals(algorithm.bagItAlgorithm)) {
					return algorithm;
				}
			}
			throw new IllegalArgumentException();
		}
		
		public static Algorithm valueOfJavaSecurityAlgorithm(String javaSecurityAlgorithm) throws IllegalArgumentException {
			for(Algorithm algorithm : Algorithm.values()) {
				if (javaSecurityAlgorithm.equals(algorithm.javaSecurityAlgorithm)) {
					return algorithm;
				}
			}
			throw new IllegalArgumentException();
		}

	
}
