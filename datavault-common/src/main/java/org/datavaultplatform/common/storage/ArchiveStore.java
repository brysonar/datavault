package org.datavaultplatform.common.storage;

import java.io.File;
import org.datavaultplatform.common.io.Progress;

/**
 * 
 * Interface for back-end archive storage systems
 *
 */
public interface ArchiveStore extends Storage {
    
    // Properties and methods relating to archive organisation and operation
    
    public VerifyMethod verificationMethod = VerifyMethod.COPY_BACK;

    VerifyMethod getVerifyMethod();
    
    // Copy an object (file/dir) from the working space
    // Progress information should be updated for monitoring as the copy occurs
    String storeToArchive(String path, File working, Progress progress) throws Exception;
    
    boolean isEncryptionEnabled();
    
    void retrieveFromArchive(String srcFileDirectoryName, File destination, Progress progress) throws Exception;
    
}
