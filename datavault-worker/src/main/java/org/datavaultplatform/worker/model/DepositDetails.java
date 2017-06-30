package org.datavaultplatform.worker.model;

import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.datavaultplatform.common.task.Task;

public class DepositDetails {

	private String jobId;
	private boolean isRedeliver;
	
	private String depositId;
	private String bagId;
	private String userId;
	private String filePath;

	public DepositDetails build(Task task) {

		jobId = task.getJobID();
		isRedeliver = task.isRedeliver();
		
		Map<String, String> properties = task.getProperties();
		depositId = properties.get("depositId");
		bagId = properties.get("bagId");
		userId = properties.get("userId");
		filePath = properties.get("filePath");

		return this;
	}

    @Override
    public String toString() {
    	   return ReflectionToStringBuilder.toString(this);
    }
    
	public String getDepositId() {
		return depositId;
	}


	public String getFilePath() {
		return filePath;
	}

	public String getJobId() {
		return jobId;
	}

	public boolean isRedeliver() {
		return isRedeliver;
	}

	public String getBagId() {
		return bagId;
	}

	public String getUserId() {
		return userId;
	}
}
