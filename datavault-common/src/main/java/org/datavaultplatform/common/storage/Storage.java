package org.datavaultplatform.common.storage;

public interface Storage {

    
    // How much space is available for storage (in bytes)
    abstract long getUsableSpace();
    
}
