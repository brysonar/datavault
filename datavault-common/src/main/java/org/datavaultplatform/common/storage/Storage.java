package org.datavaultplatform.common.storage;

import java.io.File;

import org.datavaultplatform.common.io.Progress;

public interface Storage {

    // Copy an object (file/dir) to the working space
    // Progress information should be updated for monitoring as the copy occurs
    abstract void retrieve(String path, File working, Progress progress) throws Exception;
    
    // Copy an object (file/dir) from the working space
    // Progress information should be updated for monitoring as the copy occurs
    abstract String store(String path, File working, Progress progress) throws Exception;
    
    // How much space is available for storage (in bytes)
    abstract long getUsableSpace();
    
}
