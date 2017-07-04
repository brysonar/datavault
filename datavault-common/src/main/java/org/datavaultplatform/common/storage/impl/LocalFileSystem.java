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
import org.datavaultplatform.common.io.FileCopy;
import org.datavaultplatform.common.io.Progress;
import org.datavaultplatform.common.model.FileInfo;
import org.datavaultplatform.common.storage.ArchiveStore;
import org.datavaultplatform.common.storage.Device;
import org.datavaultplatform.common.storage.VerifyMethod;
import org.datavaultplatform.common.storage.UserStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileSystem extends Device implements UserStore, ArchiveStore {

	 private static final Logger logger = LoggerFactory.getLogger(LocalFileSystem.class);
	 
    private String rootPath = null;
    
    public LocalFileSystem(String name, Map<String,String> config) throws FileNotFoundException {
        super(name, config);
        
        // Unpack the config parameters (in an implementation-specific way)
        rootPath = config.get("rootPath");
        
        // Verify parameters are correct.
        File file = new File(rootPath);
        if (!file.exists()) {
            throw new FileNotFoundException(rootPath);
        }
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
    public void retrieve(String path, File working, Progress progress) throws Exception {
    	
    	logger.debug("Local File System retrieve - path: {}", path);
    	
        Path absolutePath = getAbsolutePath(path);
        File file = absolutePath.toFile();
        
        if (file.isFile()) {
        	logger.debug("Local File System retrieve - copying file: {}", file.getAbsolutePath());
            FileCopy.copyFile(progress, file, working);
        } else if (file.isDirectory()) {
        	logger.debug("Local File System retrieve - copying directory: {}", file.getAbsolutePath());
            FileCopy.copyDirectory(progress, file, working);
        }
    }

    @Override
    public String store(String path, File working, Progress progress) throws Exception {
    	
    	logger.debug("Local File System store - path: {}", path);
    	
        Path absolutePath = getAbsolutePath(path);
        File retrieveFile = absolutePath.resolve(working.getName()).toFile();
        
        if (working.isFile()) {
            FileCopy.copyFile(progress, working, retrieveFile);
        } else if (working.isDirectory()) {
            FileCopy.copyDirectory(progress, working, retrieveFile);
        }
        
        return working.getName();
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
