	restoreNonRoundTrippedSpecFields(desired, ko)
	restoreLustreDataRepositoryFields(desired, ko)
	setLastAppliedSecretRefs(ko)
	setLastAppliedImmutableFields(ko)
