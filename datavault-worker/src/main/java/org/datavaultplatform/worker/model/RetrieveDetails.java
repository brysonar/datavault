package org.datavaultplatform.worker.model;

import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.datavaultplatform.common.task.Task;

public class RetrieveDetails {

	private String depositId;
	private String retrieveId;
	private String bagId;
	private String retrievePath;
	private String archiveId;
	private String userID;
	private String archiveDigest;
	private String archiveDigestAlgorithm;
	private long archiveSize;

	public RetrieveDetails build(Task task) {

		Map<String, String> properties = task.getProperties();
		depositId = properties.get("depositId");
		retrieveId = properties.get("retrieveId");
		bagId = properties.get("bagId");
		retrievePath = properties.get("retrievePath");
		archiveId = properties.get("archiveId");
		userID = properties.get("userId");
		archiveDigest = properties.get("archiveDigest");
		archiveDigestAlgorithm = properties.get("archiveDigestAlgorithm");
		archiveSize = Long.parseLong(properties.get("archiveSize"));

		return this;
	}

    @Override
    public String toString() {
    	   return ReflectionToStringBuilder.toString(this);
    }

	public String getDepositId() {
		return depositId;
	}

	public String getRetrieveId() {
		return retrieveId;
	}

	public String getBagId() {
		return bagId;
	}

	public String getRetrievePath() {
		return retrievePath;
	}

	public String getArchiveId() {
		return archiveId;
	}

	public String getUserID() {
		return userID;
	}

	public String getArchiveDigest() {
		return archiveDigest;
	}

	public String getArchiveDigestAlgorithm() {
		return archiveDigestAlgorithm;
	}

	public long getArchiveSize() {
		return archiveSize;
	}
    
}
