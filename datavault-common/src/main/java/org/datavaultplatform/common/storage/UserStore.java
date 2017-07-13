package org.datavaultplatform.common.storage;

import java.io.File;
import java.util.List;
import org.datavaultplatform.common.io.Progress;
import org.datavaultplatform.common.model.FileInfo;

/**
 * 
 * Interface for user facing storage systems
 *
 */
public interface UserStore extends Storage {
    
    // Properties and methods relating to user storage
    
    // List objects available under a given path
    List<FileInfo> list(String path);
    
    // Check if the passed path or resource key is allowed
    boolean valid(String path);
    
    // Check if an object exists at the specified path
    boolean exists(String path);
    
    // Get the size of an object (file/dir) in bytes
    long getSize(String path);
    
    // Get the name of an object
    String getName(String path);
    
    // Check if the passed path is a directory/container
    boolean isDirectory(String path);
    
    
    // Copy an object (file/dir) from the working space
    // Progress information should be updated for monitoring as the copy occurs
    String storeToUser(String path, File working, Progress progress) throws Exception;
    
    void retrieveFromUserStore(String srcFileDirectoryName, File destination, Progress progress) throws Exception;
}
