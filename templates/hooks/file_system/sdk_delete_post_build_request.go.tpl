	// In hooks.go because it needs the pkg/util import, which this template
	// cannot add to sdk.go.
	if err = setDeleteFileSystemConfiguration(r, input); err != nil {
		return nil, err
	}
