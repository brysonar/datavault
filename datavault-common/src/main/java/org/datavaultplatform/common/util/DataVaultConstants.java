package org.datavaultplatform.common.util;

import org.datavaultplatform.common.storage.CheckSumEnum;

public class DataVaultConstants {

	public static final boolean doTempDirectoryCleanUp = true;
	
	public static final String DATA = "data";
	public static final String MANIFEST_FILE_NAME = "manifest.txt";
	public static final String TAG_MANIFEST_FILE = "tagmanifest.txt";
	public static final String NEW_LINE = "\n";
	
	public static final String ENCRYPT_SUFFIX = ".enc";
	
	public static final CheckSumEnum CHECKSUM_ENUM = CheckSumEnum.MD5;
	public static final CheckSumEnum TAR_CHECKSUM_ENUM = CheckSumEnum.SHA1;
}
