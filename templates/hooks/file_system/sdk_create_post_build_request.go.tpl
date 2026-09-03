	// The generated builder omits a password whose referenced Secret resolves to
	// an empty value. Creation could then succeed without it while the reference
	// is recorded as applied, after which filling that Secret would never be
	// detected -- comparison tracks the reference, not the value.
	if err := rejectEmptyResolvedPasswords(desired, input); err != nil {
		return nil, err
	}
