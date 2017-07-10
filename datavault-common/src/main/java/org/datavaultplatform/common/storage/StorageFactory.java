package org.datavaultplatform.common.storage;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.datavaultplatform.common.exception.DataVaultException;
import org.datavaultplatform.common.model.FileStore;
import org.datavaultplatform.common.storage.impl.AmazonGlacier;
import org.datavaultplatform.common.storage.impl.DropboxFileSystem;
import org.datavaultplatform.common.storage.impl.LocalFileSystem;
import org.datavaultplatform.common.storage.impl.OracleCloudSystem;
import org.datavaultplatform.common.storage.impl.SFTPFileSystem;

public class StorageFactory {

	private static final String LOCAL_SYSTEM = "org.datavaultplatform.common.storage.impl.LocalFileSystem";
	private static final String AMAZON_GLACIER_SYSTEM = "org.datavaultplatform.common.storage.impl.AmazonGlacier";
	private static final String DROPBOX_FILE_SYSTEM = "org.datavaultplatform.common.storage.impl.DropboxFileSystem";
	private static final String ORACLE_CLOUD_SYSTEM = "org.datavaultplatform.common.storage.impl.OracleCloudSystem";
	private static final String SFTP_FILE_SYSTEM = "org.datavaultplatform.common.storage.impl.SFTPFileSystem";

	public ArchiveStore getArchiveStorage(String type, String name, Map<String, String> config) {

		ArchiveStore as = null;

		try {
			if (LOCAL_SYSTEM.equals(type)) {
				as = new LocalFileSystem(name, config);
			} else if (AMAZON_GLACIER_SYSTEM.equals(type)) {
				as = new AmazonGlacier(name, config);
			} else if (ORACLE_CLOUD_SYSTEM.equals(type)) {
				as = new OracleCloudSystem(name, config);
			} else {
				// not supported
				throw new DataVaultException("Archive storage type not found: " + type);
			}
		} catch (RuntimeException e) {
			throw new DataVaultException("Unable to obtain archive storage - " + e.getMessage(), e);
		}

		return as;
	}
	
	public UserStore getUserStore(String type, String name, Map<String, String> config) {

		UserStore userStore = null;

		try {
			if (LOCAL_SYSTEM.equals(type)) {
				userStore = new LocalFileSystem(name, config);
			} else if (DROPBOX_FILE_SYSTEM.equals(type)) {
				userStore = new DropboxFileSystem(name, config);
			} else if (SFTP_FILE_SYSTEM.equals(type)) {
				userStore = new SFTPFileSystem(name, config);
			} else {
				// not supported
				throw new DataVaultException("User storage type not found: " + type);
			}
		} catch (RuntimeException e) {
			throw new DataVaultException("Unable to obtain user storage - " + e.getMessage(), e);
		}

		return userStore;
	}
	
	
	public ArchiveStore getArchiveStorage(org.datavaultplatform.common.model.ArchiveStore archiveFileStore) {
		
		// Connect to the archive storage
		String storeageClass = archiveFileStore.getStorageClass();
		Map<String, String> config = archiveFileStore.getProperties();
		return getArchiveStorage(storeageClass, config);

	}

	public ArchiveStore getArchiveStorage(String storeageClass, Map<String, String> config) {
		ArchiveStore archiveFs = null;
        try {
            Class<?> clazz = Class.forName(storeageClass);
            Constructor<?> constructor = clazz.getConstructor(String.class, Map.class);
            Object instance = constructor.newInstance(storeageClass, config);
            archiveFs = (ArchiveStore)instance;
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException | RuntimeException e) {
            String errMsg = "Could not access archive filesystem";
            throw new DataVaultException(errMsg, e);
		}
		return archiveFs;
	}
	
	public UserStore getUserStorage(FileStore userFileStore) {
		
		String storeageClass = userFileStore.getStorageClass();
		Map<String, String> config = userFileStore.getProperties();
		return getUserStorage(storeageClass, config);
	}

	public UserStore getUserStorage(String storeageClass, Map<String, String> config) {
		// Connect to the user storage
        try {
            Class<?> clazz = Class.forName(storeageClass);
            Constructor<?> constructor = clazz.getConstructor(String.class, Map.class);
            Object instance = constructor.newInstance(storeageClass, config);
            UserStore userStore = (UserStore) instance;
            return userStore;
            
        } catch (Exception e) {
            String errMsg = "Could not access active filesystem";
			throw new DataVaultException(errMsg);
        }
	}
	
}
