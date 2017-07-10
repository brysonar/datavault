package org.datavaultplatform.worker.model;

import org.datavaultplatform.common.storage.ArchiveStore;
import org.datavaultplatform.common.storage.UserStore;

public class Storage {
	
	private final UserStore userStore;
	private final ArchiveStore archiveStore;
	
	public Storage(UserStore userStore, ArchiveStore archiveStore) {
		super();
		this.userStore = userStore;
		this.archiveStore = archiveStore;
	}

	public UserStore getUserStore() {
		return userStore;
	}

	public ArchiveStore getArchiveStore() {
		return archiveStore;
	}

	
}

