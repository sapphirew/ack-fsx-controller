	// Delete is async: FSx stays DELETING for 5-10 min while its ENIs are torn
	// down, so requeue instead of dropping the finalizer -- otherwise the ec2
	// Subnet/SecurityGroup CRs this references fail to delete. Later reconciles
	// short-circuit in sdk_delete_pre_build_request until FileSystemNotFound.
	if err == nil {
		return r, requeueWaitWhileDeleting
	}
