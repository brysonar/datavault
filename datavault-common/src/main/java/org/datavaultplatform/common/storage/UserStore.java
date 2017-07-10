package org.datavaultplatform.common.storage;

// Interface for user facing storage systems

import java.util.List;
import org.datavaultplatform.common.model.FileInfo;


public interface UserStore extends Storage {
    
    // Properties and methods relating to user storage
    
    // List objects available under a given path
    public List<FileInfo> list(String path);
    
    // Check if the passed path or resource key is allowed
    public boolean valid(String path);
    
    // Check if an object exists at the specified path
    public boolean exists(String path);
    
    // Get the size of an object (file/dir) in bytes
    public long getSize(String path);
    
    // Get the name of an object
    public String getName(String path);
    
    // Check if the passed path is a directory/container
    public boolean isDirectory(String path);
}
