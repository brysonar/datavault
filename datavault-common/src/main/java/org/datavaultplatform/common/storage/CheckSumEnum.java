package org.datavaultplatform.common.storage;


public enum CheckSumEnum {

		MD5 ("md5", "MD5"), SHA1 ("sha1", "SHA-1"), SHA256 ("sha256", "SHA-256"), SHA512 ("sha512", "SHA-512");
		
		private String bagItAlgorithm;
		private String javaSecurityAlgorithm;
		
		CheckSumEnum(String bagItAlgorithm, String javaSecurityAlgorithm) {
			this.bagItAlgorithm = bagItAlgorithm;
			this.javaSecurityAlgorithm = javaSecurityAlgorithm;
		}
		
		public static CheckSumEnum valueOfBagItAlgorithm(String bagItAlgorithm) throws IllegalArgumentException {
			for(CheckSumEnum algorithm : CheckSumEnum.values()) {
				if (bagItAlgorithm.equals(algorithm.bagItAlgorithm)) {
					return algorithm;
				}
			}
			throw new IllegalArgumentException();
		}
		
		public static CheckSumEnum valueOfJavaSecurityAlgorithm(String javaSecurityAlgorithm) throws IllegalArgumentException {
			for(CheckSumEnum algorithm : CheckSumEnum.values()) {
				if (javaSecurityAlgorithm.equals(algorithm.javaSecurityAlgorithm)) {
					return algorithm;
				}
			}
			throw new IllegalArgumentException();
		}

		public String getBagItAlgorithm() {
			return bagItAlgorithm;
		}

		public String getJavaSecurityAlgorithm() {
			return javaSecurityAlgorithm;
		}

	
}
