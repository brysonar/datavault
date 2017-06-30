package org.datavaultplatform.worker.model;

import org.datavaultplatform.common.storage.Device;
import org.datavaultplatform.common.storage.UserStore;

public class UserStorage {
	
	private final Device userFs;
	private final UserStore userStore;
	
	public UserStorage(Device userFs, UserStore userStore) {
		super();
		this.userFs = userFs;
		this.userStore = userStore;
	}

	public Device getUserFs() {
		return userFs;
	}

	public UserStore getUserStore() {
		return userStore;
	}

}
