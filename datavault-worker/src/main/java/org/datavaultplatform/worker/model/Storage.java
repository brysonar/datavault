package org.datavaultplatform.worker.model;

import org.datavaultplatform.common.storage.ArchiveStore;

public class Storage {
	
	private final UserStorage userStorage;
	private final ArchiveStore archiveFs;
	
	public Storage(UserStorage userStorage, ArchiveStore archiveFs) {
		super();
		this.userStorage = userStorage;
		this.archiveFs = archiveFs;
	}

	public UserStorage getUserStorage() {
		return userStorage;
	}

	public ArchiveStore getArchiveFs() {
		return archiveFs;
	}
	
}

