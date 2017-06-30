package org.datavaultplatform.worker.model;

import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class MetaData {
	
	private String depositMetadata;
	private String vaultMetadata;
	private String externalMetadata;
	
	public MetaData build(Map<String, String> properties) {
		
		 depositMetadata = properties.get("depositMetadata");
		 vaultMetadata = properties.get("vaultMetadata");
		 externalMetadata = properties.get("externalMetadata");
		 
		 return this;
	}

    @Override
    public String toString() {
    	   return ReflectionToStringBuilder.toString(this);
    }
    
	public String getDepositMetadata() {
		return depositMetadata;
	}

	public String getVaultMetadata() {
		return vaultMetadata;
	}

	public String getExternalMetadata() {
		return externalMetadata;
	}
	
	
}
