// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package util

import (
	"fmt"
	"strconv"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
)

// DeleteInputAnnotationParameters holds delete-time options read from
// annotations rather than the Spec. They apply only at deletion and Describe
// never returns them, so a Spec field would report a delta on every reconcile;
// annotations sit outside the delta machinery.
type DeleteInputAnnotationParameters struct {
	// Neither field is nil after a successful parse.
	SkipFinalBackup *bool
	CascadeDelete   *bool
}

// See SkipFinalBackupAnnotation for why the default is to skip.
var defaultSkipFinalBackup = true

// See CascadeDeleteAnnotation for why cascading deletion is opt-in.
var defaultCascadeDelete = false

// ParseDeletionAnnotations parses the deletion annotations, defaulting any that
// are absent. An unparseable value is an error rather than a silent default, so
// a typo cannot quietly destroy a final backup the user asked for.
func ParseDeletionAnnotations(annotations map[string]string) (*DeleteInputAnnotationParameters, error) {
	params := &DeleteInputAnnotationParameters{
		SkipFinalBackup: &defaultSkipFinalBackup,
		CascadeDelete:   &defaultCascadeDelete,
	}
	if len(annotations) == 0 {
		return params, nil
	}

	if value, ok := annotations[svcapitypes.SkipFinalBackupAnnotation]; ok && value != "" {
		skipFinalBackup, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid value %q for annotation %s: %v",
				value, svcapitypes.SkipFinalBackupAnnotation, err,
			)
		}
		params.SkipFinalBackup = &skipFinalBackup
	}

	if value, ok := annotations[svcapitypes.CascadeDeleteAnnotation]; ok && value != "" {
		cascadeDelete, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid value %q for annotation %s: %v",
				value, svcapitypes.CascadeDeleteAnnotation, err,
			)
		}
		params.CascadeDelete = &cascadeDelete
	}

	return params, nil
}
