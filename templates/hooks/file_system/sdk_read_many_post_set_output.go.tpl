	restoreNonRoundTrippedSpecFields(r, ko)
	recoverObservedLustreDataRepository(r, resp, ko)
	ensureLastAppliedSecretBaseline(ko)
	ensureImmutableFieldBaseline(ko)
	if err := terminalIfFailed(ko); err != nil {
		return &resource{ko}, err
	}
