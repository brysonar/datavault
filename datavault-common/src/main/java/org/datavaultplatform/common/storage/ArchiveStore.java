package org.datavaultplatform.common.storage;

// Interface for back-end archive storage systems

public interface ArchiveStore {
    
    // Properties and methods relating to archive organisation and operation
    
    public VerifyMethod verificationMethod = VerifyMethod.COPY_BACK;
    public VerifyMethod getVerifyMethod();
}
