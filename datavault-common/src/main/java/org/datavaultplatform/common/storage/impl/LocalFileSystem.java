package org.datavaultplatform.common.storage.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.datavaultplatform.common.exception.DataVaultException;
import org.datavaultplatform.common.io.Progress;
import org.datavaultplatform.common.model.FileInfo;
import org.datavaultplatform.common.storage.ArchiveStore;
import org.datavaultplatform.common.storage.Device;
import org.datavaultplatform.common.storage.UserStore;
import org.datavaultplatform.common.storage.VerifyMethod;
import org.datavaultplatform.common.util.DataVaultConstants;
import org.datavaultplatform.common.util.EncryptionCryptoUtil;
import org.datavaultplatform.common.util.FileCopy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileSystem extends Device implements UserStore, ArchiveStore {

	private static final Logger logger = LoggerFactory.getLogger(LocalFileSystem.class);

	private String rootPath = null;

	public LocalFileSystem(String name, Map<String, String> config) {
		super(name, config);

		// Unpack the config parameters (in an implementation-specific way)
		rootPath = config.get("rootPath");

		// Verify parameters are correct.
		File file = new File(rootPath);
		if (!file.exists()) {
			throw new DataVaultException("File not found: " + rootPath, new FileNotFoundException(rootPath));
		}
	}
    
	@Override
	public boolean isEncryptionEnabled() {
		return true;
	}
	
    @Override
    public List<FileInfo> list(String path) {
        
        Path basePath = Paths.get(rootPath);
        Path completePath = getAbsolutePath(path);

        if (completePath == null) {
            throw new IllegalArgumentException("Path invalid");
        }

        ArrayList<FileInfo> files = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(completePath)) {
            for (Path entry : stream) {
                
                String entryFileName = entry.getFileName().toString();
                String entryAbsolutePath = entry.toString();
                
                // The "key" is the path under the base directory.
                // The API client can use this to request a sub-directory.
                String entryKey = (basePath.toUri().relativize(entry.toUri())).getPath();
                
                FileInfo info = new FileInfo(entryKey,
                                             entryAbsolutePath,
                                             entryFileName,
                                             Files.isDirectory(entry));
                files.add(info);
            }

        } catch (IOException e) {
        	//TODO do we really want to swallow error
        	logger.error(e.getMessage(), e);
        }

        return files;
    }
    
    @Override
    public boolean valid(String path) {
        Path absolutePath = getAbsolutePath(path);
        return (absolutePath != null);
    }

    @Override
    public boolean exists(String path) {
        Path absolutePath = getAbsolutePath(path);
        File file = absolutePath.toFile();
        return file.exists();
    }
    
    @Override
    public long getSize(String path) {
        Path absolutePath = getAbsolutePath(path);
        File file = absolutePath.toFile();
        
        if (file.isDirectory()) {
            return FileUtils.sizeOfDirectory(file);
        } else {
            return FileUtils.sizeOf(file);
        }
    }

    @Override
    public boolean isDirectory(String path) {
        Path absolutePath = getAbsolutePath(path);
        File file = absolutePath.toFile();
        return file.isDirectory();
    }
    
    @Override
    public String getName(String path) {
        Path absolutePath = getAbsolutePath(path);
        File file = absolutePath.toFile();
        return file.getName();
    }
    
    @Override
    public long getUsableSpace() {
        File file = new File(rootPath);
        return file.getUsableSpace();
    }

    @Override
    public void retrieveFromUserStore(String srcFileDirectoryName, File destination, Progress progress) {
    	
    	logger.debug("Local File System retrieve from User Store - path: {} to {}", srcFileDirectoryName, destination.getAbsolutePath());
    	
        Path srcPath = getAbsolutePath(srcFileDirectoryName);
        File srcfile = srcPath.toFile();
        
        if (srcfile.isFile()) {
        	logger.debug("Local File System retrieve - copying file: {}", srcPath);
            copyFile(progress, srcfile, destination);
        } else if (srcfile.isDirectory()) {
        	logger.debug("Local File System retrieve - copying directory: {}", srcPath);
            copyDirectory(progress, srcfile, destination);
        }
    }
    
    @Override
    public void retrieveFromArchive(String archiveFileName, File destination, Progress progress) throws Exception {
    	
    	logger.debug("Local File System retrieve from archive - archive file name: {} to {}", archiveFileName, destination.getAbsolutePath());
    	
        Path archivePath = getAbsolutePath(archiveFileName);
        File archivefile = archivePath.toFile();
        
        if (archivefile.isDirectory()) {
        	
        	logger.debug("Local File System retrieve - copying directory: {}", archivePath);
            copyDirectory(progress, archivefile, destination);
            
        } else {
        	logger.debug("Local File System retrieve - copying file: {}", archivePath);
        	if (isEncryptionEnabled()) {
        
        		File archivedEncryptedFile = archivePath.getParent().resolve(destination.getName() + DataVaultConstants.ENCRYPT_SUFFIX).toFile();
        		validateFileExists(archivedEncryptedFile);
        		File destinationEncryptedFile = destination.getParentFile().toPath().resolve(destination.getName() + DataVaultConstants.ENCRYPT_SUFFIX).toFile();
            	copyFile(progress, archivedEncryptedFile, destinationEncryptedFile);
            	EncryptionCryptoUtil.decrypt(destinationEncryptedFile, destination);
            	
            	boolean result = destinationEncryptedFile.delete();
            	logger.debug("Deleted encrypted file {}: {}", destinationEncryptedFile, result);
        	} else {
        		validateFileExists(archivefile);
        		copyFile(progress, archivefile, destination);
        	}
        } 
    }

	private void validateFileExists(File file) {
		if (!file.exists()) {
			throw new DataVaultException("File " + file.getAbsolutePath() + " not found");
		}
		if (!file.isFile()) {
			throw new DataVaultException(file.getAbsolutePath() +  " is not a file");
		}
	}

    @Override
    public String storeToUser(String destination, File srcFile, Progress progress) {
    	
    	logger.debug("Copy file from {} to User Store: {}", srcFile.getAbsolutePath(), destination);

        Path destinationPath = getAbsolutePath(destination);

        if (srcFile.isFile()) {
        	
        	if (srcFile.getName().endsWith(DataVaultConstants.ENCRYPT_SUFFIX)) {
        		//decrypt file
        		String decryptedFileName = srcFile.getName().substring(0, srcFile.getName().length()-4);
        		logger.debug("decryptedFileName: " + decryptedFileName);
        		File decryptedFile = srcFile.getParentFile().toPath().resolve(decryptedFileName).toFile();
        		EncryptionCryptoUtil.decrypt(srcFile, decryptedFile);
        	}
        	copyFile(progress, srcFile, destinationPath);
        } else if (srcFile.isDirectory()) {
            copyDirectory(progress, srcFile, destinationPath);
        }
        
        return srcFile.getName();
    }
    
    @Override
    public String storeToArchive(String destination, File srcFile, Progress progress) {
    	
    	logger.debug("Copy file from {} to Archive Store: {}", srcFile.getAbsolutePath(), destination);

        Path destinationPath = getAbsolutePath(destination);

        if (srcFile.isFile()) {
        	//encrypt
        	
        	if (isEncryptionEnabled()) {
        		Path directory = srcFile.getParentFile().toPath();
            	File encryptedFile = directory.resolve(srcFile.getName() + DataVaultConstants.ENCRYPT_SUFFIX).toFile();
            	logger.debug("encryptedFile: " + encryptedFile.getAbsolutePath());
            	EncryptionCryptoUtil.encrypt(srcFile, encryptedFile);
            	copyFile(progress, encryptedFile, destinationPath);
        	} else {
        		copyFile(progress, srcFile, destinationPath);
        	}
        } else if (srcFile.isDirectory()) {
            copyDirectory(progress, srcFile, destinationPath);
        }
        
        return srcFile.getName();
    }


	private void copyFile(Progress progress, File srcFile, Path destinationPath) {
		File destinationFile = destinationPath.resolve(srcFile.getName()).toFile();
		copyFile(progress, srcFile, destinationFile);
	}

	private void copyFile(Progress progress, File srcFile, File destinationFile) {
		try {
			FileCopy.copyFile(progress, srcFile, destinationFile);
		} catch (IOException e) {
			throw new DataVaultException("Failed to copy file " + srcFile.getAbsolutePath() + " - "  + e.getMessage(), e);
		}
	}

	private void copyDirectory(Progress progress, File srcFile,  Path destinationPath) {
		File destinationFile = destinationPath.resolve(srcFile.getName()).toFile();
		copyDirectory(progress, srcFile, destinationFile);
	}

	private void copyDirectory(Progress progress, File srcFile, File destinationFile) {
		try {
			FileCopy.copyDirectory(progress, srcFile, destinationFile);
		} catch (IOException e) {
			throw new DataVaultException("Failed to copy directory " + srcFile.getAbsolutePath() + " - "  + e.getMessage(), e);
		}
	}
    
    
    @Override
    public VerifyMethod getVerifyMethod() {
        // Return the default verification method (copy back and check)
        return verificationMethod;
    }
    
    private Path getAbsolutePath(String filePath) {
        
        // Join the requested path to the root of the filesystem.
        // In future this path handling should be part of a filesystem-specific driver.
        Path base = Paths.get(rootPath);
        Path absolute;
        
        try {
            if (filePath.equals("")) {
                absolute = base;
            } else {
                // A leading '/' would cause the path to be treated as absolute
                while (filePath.startsWith("/")) {
                    filePath = filePath.replaceFirst("/", "");
                }

                absolute = base.resolve(filePath);
                absolute = Paths.get(absolute.toFile().getCanonicalPath());
            }

            if (isValidSubPath(absolute)) {
                return absolute;
            } else {
                // Path is invalid (doesn't exist in base)!
                return null;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }
    
    private boolean isValidSubPath(Path path) {
        
        // Check if the path is valid with respect to the base path.
        // For example, we don't want to allow path traversal ("../../abc").
        
        try {
            Path base = Paths.get(rootPath);
            Path canonicalBase = Paths.get(base.toFile().getCanonicalPath());
            Path canonicalPath = Paths.get(path.toFile().getCanonicalPath());
            
            if (canonicalPath.startsWith(canonicalBase)) {
                return true;
            } else {
                return false;
            }
        }
        catch (Exception e) {
        	logger.error(e.getMessage(), e);
            return false;
        }
    }
}
