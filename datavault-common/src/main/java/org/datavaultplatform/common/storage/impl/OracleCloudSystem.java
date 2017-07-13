package org.datavaultplatform.common.storage.impl;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import org.datavaultplatform.common.exception.DataVaultException;
import org.datavaultplatform.common.io.Progress;
import org.datavaultplatform.common.storage.ArchiveStore;
import org.datavaultplatform.common.storage.Device;
import org.datavaultplatform.common.storage.VerifyMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.cloudstorage.ftm.CloudStorageClass;
import oracle.cloudstorage.ftm.DownloadConfig;
import oracle.cloudstorage.ftm.FileTransferAuth;
import oracle.cloudstorage.ftm.FileTransferManager;
import oracle.cloudstorage.ftm.MultiFileTransferResult;
import oracle.cloudstorage.ftm.TransferResult;
import oracle.cloudstorage.ftm.TransferTask;
import oracle.cloudstorage.ftm.UploadConfig;
import oracle.cloudstorage.ftm.exception.ClientException;

//import oracle.cloud.storage.*;
//import oracle.cloud.storage.model.*;
//import oracle.cloud.storage.exception.*;

public class OracleCloudSystem extends Device implements ArchiveStore {
	
	private static final Logger logger = LoggerFactory.getLogger(OracleCloudSystem.class);

	public OracleCloudSystem(String name, Map<String, String> config) {
		super(name, config);
	}


	@Override
	public boolean isEncryptionEnabled() {
		return true;
	}
	
	private FileTransferAuth buildFileTransferAuth() {
		
//		FileTransferAuth auth = new FileTransferAuth(
//				"john.doe@oracle.com", // user name
//				"Welcome1!".toCharArray(), // password
//				"storage", //  service name
//				"https://storagedomain.storage.oraclecloud.com", // service URL
//				"storagedomain" // identity domain
//		);
//		
//		Properties prop = new Properties();
//		
//		FileTransferAuth auth2 = new FileTransferAuth(
//				prop.getProperty("user-name"),
//				prop.getProperty("password").toCharArray(),
//				prop.getProperty("service-name"), 
//				prop.getProperty("service-url"), 
//				prop.getProperty("identity-domain")
//				);
		
		FileTransferAuth auth3 = new FileTransferAuth(
				config.get("user-name"),
				config.get("password").toCharArray(),
				config.get("service-name"), 
				config.get("service-url"), 
				config.get("identity-domain")
				);

		return auth3;
	}
	
	@Override
	public String storeToArchive(String destination, File srcFile, Progress progress) {

		String containerName = "container_name";
		boolean isAsync = true;
		boolean isDirectory = false;
		
		upload(containerName, srcFile, isDirectory, isAsync);
	    return srcFile.getName();
	}
	
	private void upload(String containerName, File file, boolean isDirectory, boolean isAsync) {

		FileTransferManager manager = null;
		try {
			manager = FileTransferManager.getDefaultFileTransferManager(buildFileTransferAuth());
			UploadConfig uploadConfig = new UploadConfig();
			uploadConfig.setOverwrite(true);
			
			//TODO do i need the object name
			String objectName = null;
			//String objectName = file.getName();
			
			//TODO - do i need segmentSize
			//should i use Standard or Archive
//			uploadConfig.setStorageClass(CloudStorageClass.Archive);
//			uploadConfig.setSegmentSize(1024 * 1024);
			
			uploadConfig.setStorageClass(CloudStorageClass.Standard);

			if (!isDirectory) {
				if (isAsync) {
					TransferTask<TransferResult> uploadTask = manager.uploadAsync(uploadConfig, containerName,
							objectName, file);
					logger.info("Waiting for upload task to complete...");
					TransferResult uploadResult = uploadTask.getResult();
					logger.info("Task completed. State:" + uploadResult.getState());
				} else {
					TransferResult uploadResult = manager.upload(uploadConfig, containerName, objectName, file);
					// uploadResult.getHttpStatusCode()
//					uploadResult.getMessage()
//					uploadResult.getRestoreCompletedPercentage()
					logger.info("Task completed. State:" + uploadResult.getState());
				}
			} else {
				if (isAsync) {
					logger.info("Waiting for upload directory task to complete...");
					File dir = file;
					TransferTask<MultiFileTransferResult> uploadTask = manager.uploadDirectoryAsync(uploadConfig, containerName, null,
							dir, false);
					MultiFileTransferResult uploadResult = uploadTask.getResult();
					logger.info("Upload completed. " + uploadResult.toString());
				} else {
					File dir = file;
					MultiFileTransferResult uploadResult = manager.uploadDirectory(uploadConfig, containerName, null,
							dir, false);
					logger.info("Upload completed. " + uploadResult.toString());
				}
			}

		} catch (ClientException e) {
			logger.error("Operation failed. " + e.getMessage());
			throw new DataVaultException(e.getMessage(), e);
		} finally {
			if (manager != null) {
				manager.shutdown();
			}
		}
	}
	


	//http://www.oracle.com/webfolder/technetwork/tutorials/obe/cloud/objectstorage/upload_files_gt_5GB_FTM_API/upload_files_gt_5GB_FTM_API.html

	
//	 String containerName = "myContainer";
//     File file = new File("sample_5MB.pdf");
//     UploadConfig uploadConfig = new UploadConfig();
//     uploadConfig.setOverwrite(true);
//     uploadConfig.setStorageClass(CloudStorageClass.Standard);
//     uploadConfig.setSegmentsContainer("myContainer-Segments");
//     uploadConfig.setSegmentSize(1048576);
     
//	public class FileMd5Calculator{
//	    public static String checkSum(String filepath);
//	}
	
//	 FileMd5Calculator myMd5Calculator = new FileMd5Calculator();
//     
//     String originalFileMd5 = myMd5Calculator.checkSum(file.getAbsolutePath()); 
//     String concatenatedFileMd5 = myMd5Calculator.checkSum(downloadfile.getAbsolutePath()); 
//
//     System.out.println("\nThe original file's MD5 is: " + originalFileMd5);
//     System.out.println("The downloaded file's MD5 is: " + concatenatedFileMd5);
     
	
//	private void testUpload() {
//		System.out.println("Uploading file " + file.getName() + " to container " + containerName);
//        TransferResult uploadResult = manager.upload(uploadConfig, containerName, null, file);
//        System.out.println("Upload completed. Result:" + uploadResult.toString());
//	}
	
	private void t() {
		//AccountMetadata d = new AccountMetadata();
		
	
		String restoreDirPath = "restoredfiles"; 
		
        File restoreDir = new File(restoreDirPath);
        if (!restoreDir.isDirectory()) {
            restoreDir.mkdir();
        }
        
        String objectName = "sample_5MB.pdf";
        File downloadfile = new File(restoreDirPath + File.separator + "concatenated-" + objectName);
        DownloadConfig downloadConfig = new DownloadConfig();
//        System.out.println("Downloading file " + downloadfile.getName() + " from container " + containerName);
//        TransferResult downloadResult = manager.download(downloadConfig, containerName, objectName, downloadfile);
//        System.out.println("Download completed. State:" + downloadResult.getState());
	}
	

	//http://docs.oracle.com/en/cloud/iaas/storage-cloud/cssto/listing-containers.html
	
//	curl -v -X GET \
//    -H "X-Auth-Token: token" \
//    accountURL[?query_parameter=value]
    		
//	curl -v -X GET \
//    -H "X-Auth-Token: AUTH_tkb4fdf39c92e9f62cca9b7c196f8b6e6b" \
//    https://foo.storage.oraclecloud.com/v1/myservice-bar?limit=15
    	
	
// http://www.oracle.com/webfolder/technetwork/tutorials/obe/cloud/objectstorage/upload_files_gt_5GB_REST_API/upload_files_gt_5GB_REST_API.html	
//	curl -v -s -X GET -H "X-Storage-User: myService-myIdentityDomain:john.doe@oracle.com" -H "X-Storage-Pass: xUs8M8rw" https://foo.storage.oraclecloud.com/auth/v1.0
	
//	curl -v -X HEAD \
//    -H "X-Auth-Token: AUTH_tkbaebb60dfa5b80d84e62b0d5d07031e5" 
//https://foo.storage.oraclecloud.com/v1/Storage-myIdentityDomain/FirstContainer/myLargeFile
	
	
//	curl -v -X GET \
//    -H "X-Auth-Token: AUTH_tk5a58b7a8c34bb7b662523a59a5272650" 
//    "https://foo.storage.oraclecloud.com/v1/Storage-myIdentityDomain/FirstContainer/myLargeFile" \
//    -o ./myLargeFile1
    
    
//	http://docs.oracle.com/en/cloud/iaas/storage-cloud/cssto/getting-container-metadata.html
//	curl -v -X HEAD \
//    -H "X-Auth-Token: AUTH_tkb4fdf39c92e9f62cca9b7c196f8b6e6b" \
//    https://foo.storage.oraclecloud.com/v1/myservice-bar/FirstContainer		
	
	
	//https://docs.oracle.com/en/cloud/iaas/storage-cloud/cssto/accessing-oracle-storage-cloud-service.html#GUID-B431E096-06B5-4FB5-B429-8CE95585BB25
	
	@Override
	public void retrieveFromArchive(String srcFileDirectoryName, File destination, Progress progress) {

		String containerName = "ftmapi-demo";
		String objectName = "fmtest.txt";
		String restoreDirPath = "restoredfiles";
		download(containerName, objectName, restoreDirPath);
	}
	
	private void download(final String containerName, final String objectName, final String restoreDirPath) {

		FileTransferManager manager = null;
		try {
			manager = FileTransferManager.getDefaultFileTransferManager(buildFileTransferAuth());
			// Create the restoreDirPath if required
			File restoreDir = new File(restoreDirPath);
			if (!restoreDir.isDirectory()) {
				restoreDir.mkdir();
			}

			File file = new File(restoreDirPath + File.separator + objectName);
			
			DownloadConfig downloadConfig = new DownloadConfig();
			
			logger.info("Downloading file " + file.getName() + " from container " + containerName);
			TransferResult uploadResult = manager.download(downloadConfig, containerName, objectName, file);
			logger.info("Download completed. State:" + uploadResult.getState());
			
//			MultiFileTransferResult downloadResult = manager.downloadDirectory(downloadConfig, containerName, null, dir);

		} catch (ClientException e) {
			logger.error("Operation failed. " + e.getMessage());
			throw new DataVaultException(e.getMessage(), e);
		} finally {
			if (manager != null) {
				manager.shutdown();
			}
		}
	}

	@Override
	public long getUsableSpace() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public VerifyMethod getVerifyMethod() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	
	
	
//	public class UploadFileDemo {
//	    private static final String className = UploadFileDemo.class.getSimpleName();
//	    private static final String demoAccountPropertiesFilepath = "my-account.properties";
//	    private static final String restoreDirPath = "restoredfiles";
//
//	    public static void main(String[] args) throws Exception {
//	        Properties prop = new Properties();
//	        try (InputStream is = new FileInputStream(demoAccountPropertiesFilepath)) {
//	            prop.load(is);
//	        } catch (Exception e) {
//	            System.out.println("Failed to read demo account properties file.");
//	            throw e;
//	        }
//	        FileTransferAuth auth = new FileTransferAuth(prop.getProperty("user-name"), prop.getProperty("password"),
//	                prop.getProperty("service-name"), prop.getProperty("service-url"), prop.getProperty("identity-domain"));
//
//	        FileTransferManager manager = null;
//	        try {
//	            manager = FileTransferManager.getDefaultFileTransferManager(auth);
//
//	            String containerName = "myContainer";
//	            File file = new File("sample_5MB.pdf");
//	            UploadConfig uploadConfig = new UploadConfig();
//	            uploadConfig.setOverwrite(true);
//	            uploadConfig.setStorageClass(CloudStorageClass.Standard);
//	            uploadConfig.setSegmentsContainer("myContainer-Segments");
//	            uploadConfig.setSegmentSize(1048576);
//	            System.out.println("Uploading file " + file.getName() + " to container " + containerName);
//	            TransferResult uploadResult = manager.upload(uploadConfig, containerName, null, file);
//	            System.out.println("Upload completed. Result:" + uploadResult.toString());
//	                        
//	            //Download begins here
//	            // Create the restoreDirPath if required
//	            File restoreDir = new File(restoreDirPath);
//	            if (!restoreDir.isDirectory()) {
//	                    restoreDir.mkdir();
//	            }
//	            String objectName = "sample_5MB.pdf";
//	            File downloadfile = new File(restoreDirPath + File.separator + "concatenated-" + objectName);
//	            DownloadConfig downloadConfig = new DownloadConfig();
//	            System.out.println("Downloading file " + downloadfile.getName() + " from container " + containerName);
//	            TransferResult downloadResult = manager.download(downloadConfig, containerName, objectName, downloadfile);
//	            System.out.println("Download completed. State:" + downloadResult.getState());
//	                    
//	            //check MD5 checksum here
//	            FileMd5Calculator myMd5Calculator = new FileMd5Calculator();
//	                        
//	            String originalFileMd5 = myMd5Calculator.checkSum(file.getAbsolutePath()); 
//	            String concatenatedFileMd5 = myMd5Calculator.checkSum(downloadfile.getAbsolutePath()); 
//
//	            System.out.println("\nThe original file's MD5 is: " + originalFileMd5);
//	            System.out.println("The downloaded file's MD5 is: " + concatenatedFileMd5);
//	            if (originalFileMd5.equals(concatenatedFileMd5)) {
//	                System.out.println("\nSUCCESS: The original file and the downloaded file are identical!");
//	            } else {
//	                System.out.println("\nERROR: The original file and the downloaded file are NOT identical!");
//	            }
//	        } catch (ClientException ce) {
//	            System.out.println("Operation failed. " + ce.getMessage());
//	        } finally {
//	            if (manager != null) {
//	                                manager.shutdown();
//	            }
//	        }
//	    }
//	}

}
