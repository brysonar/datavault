package org.datavaultplatform.common.storage;

import java.util.Map;

// A generic storage UserStore/system

public abstract class Device {
    
    // Some public information about a device or storage system
    public String name;
    
    // Some private configuration properties
    protected Map<String,String> config;
    
    public Device(String name, Map<String,String> config) {
        this.name = name;
        this.config = config;
    }

}
