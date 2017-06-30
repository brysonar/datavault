package org.datavaultplatform.common.model;

import java.util.HashMap;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.hibernate.annotations.GenericGenerator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@Table(name="FileStores")
public class FileStore {

    // Storage Identifier
    @Id
    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    @Column(name = "id", unique = true, length = 36)
    private String id;

    // Class to use for access to storage
    @Column(columnDefinition = "TEXT")
    private String storageClass;
    
    // A textual description of the storage system
    @Column(columnDefinition = "TEXT")
    private String label;
    
    // Properties to use for this storage system
    // NOTE: this is not a secure mechanism for storing credentials!
    @Lob
    private HashMap<String,String> properties;
    
    @JsonIgnore
    @ManyToOne
    private User user;
    
    public FileStore() {}
    
    public FileStore(String storageClass, HashMap<String,String> properties, String label) {
        this.storageClass = storageClass;
        this.properties = properties;
        this.label = label;
    }

    @Override
    public String toString() {
    	   return ReflectionToStringBuilder.toString(this);
    }
    
    public String getID() { return id; }

    public String getStorageClass() { return storageClass; }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }

    public void setProperties(HashMap<String, String> properties) {
        this.properties = properties;
    }

    public String getLabel() { return label; }

    public void setLabel(String label) {
        this.label = label;
    }
    
    public User getUser() { return user; }
    
    public void setUser(User user) {
        this.user = user;
    }
}
