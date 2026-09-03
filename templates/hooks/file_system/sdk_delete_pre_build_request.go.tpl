	// ReadOne runs immediately before Delete, so Status.Lifecycle is current.
	// FSx rejects DeleteFileSystem for a file system already DELETING, so
	// requeue rather than making a call that can only fail. Return `r` rather
	// than nil (as rds/eks/elasticache/memorydb do) so the runtime has a
	// resource to patch status from during the wait.
	if isDeleting(r) {
		return r, requeueWaitWhileDeleting
	}
