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
	"testing"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
)

func TestParseDeletionAnnotations(t *testing.T) {
	tests := []struct {
		name                string
		annotations         map[string]string
		wantSkipFinalBackup bool
		wantCascadeDelete   bool
		wantErr             bool
	}{
		{
			name:                "nil annotations default to skipping",
			annotations:         nil,
			wantSkipFinalBackup: true,
		},
		{
			name:                "empty annotations default to skipping",
			annotations:         map[string]string{},
			wantSkipFinalBackup: true,
		},
		{
			name: "unrelated annotations default to skipping",
			annotations: map[string]string{
				"example.com/some-other-annotation": "false",
			},
			wantSkipFinalBackup: true,
		},
		{
			name: "empty value falls back to the default",
			annotations: map[string]string{
				svcapitypes.SkipFinalBackupAnnotation: "",
			},
			wantSkipFinalBackup: true,
		},
		{
			name: "false opts into a final backup",
			annotations: map[string]string{
				svcapitypes.SkipFinalBackupAnnotation: "false",
			},
			wantSkipFinalBackup: false,
		},
		{
			name: "true skips the final backup",
			annotations: map[string]string{
				svcapitypes.SkipFinalBackupAnnotation: "true",
			},
			wantSkipFinalBackup: true,
		},
		{
			name: "ParseBool accepts 0 as false",
			annotations: map[string]string{
				svcapitypes.SkipFinalBackupAnnotation: "0",
			},
			wantSkipFinalBackup: false,
		},
		{
			name: "ParseBool accepts capitalised False",
			annotations: map[string]string{
				svcapitypes.SkipFinalBackupAnnotation: "False",
			},
			wantSkipFinalBackup: false,
		},
		{
			name: "an unparseable value is an error, not a silent default",
			annotations: map[string]string{
				svcapitypes.SkipFinalBackupAnnotation: "nope",
			},
			wantErr: true,
		},
		{
			name:                "cascade delete defaults off",
			annotations:         nil,
			wantSkipFinalBackup: true,
			wantCascadeDelete:   false,
		},
		{
			name: "cascade delete opt-in",
			annotations: map[string]string{
				svcapitypes.CascadeDeleteAnnotation: "true",
			},
			wantSkipFinalBackup: true,
			wantCascadeDelete:   true,
		},
		{
			name: "cascade delete explicit false",
			annotations: map[string]string{
				svcapitypes.CascadeDeleteAnnotation: "false",
			},
			wantSkipFinalBackup: true,
			wantCascadeDelete:   false,
		},
		{
			name: "both annotations parsed independently",
			annotations: map[string]string{
				svcapitypes.SkipFinalBackupAnnotation: "false",
				svcapitypes.CascadeDeleteAnnotation:   "true",
			},
			wantSkipFinalBackup: false,
			wantCascadeDelete:   true,
		},
		{
			name: "an unparseable cascade-delete value is an error",
			annotations: map[string]string{
				svcapitypes.CascadeDeleteAnnotation: "yes-please",
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params, err := ParseDeletionAnnotations(test.annotations)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got params %+v", params)
				}
				if params != nil {
					t.Errorf("expected nil params alongside the error, got %+v", params)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if params.SkipFinalBackup == nil {
				t.Fatal("SkipFinalBackup should never be nil after a successful parse")
			}
			if got := *params.SkipFinalBackup; got != test.wantSkipFinalBackup {
				t.Errorf("SkipFinalBackup = %v, want %v", got, test.wantSkipFinalBackup)
			}
			if params.CascadeDelete == nil {
				t.Fatal("CascadeDelete should never be nil after a successful parse")
			}
			if got := *params.CascadeDelete; got != test.wantCascadeDelete {
				t.Errorf("CascadeDelete = %v, want %v", got, test.wantCascadeDelete)
			}
		})
	}
}

// TestParseDeletionAnnotationsDoesNotMutateDefault guards against the parsed
// value aliasing the package-level default, which would let one resource's
// annotation change the default applied to every subsequent resource.
func TestParseDeletionAnnotationsDoesNotMutateDefault(t *testing.T) {
	params, err := ParseDeletionAnnotations(map[string]string{
		svcapitypes.SkipFinalBackupAnnotation: "false",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *params.SkipFinalBackup != false {
		t.Fatalf("SkipFinalBackup = true, want false")
	}

	defaults, err := ParseDeletionAnnotations(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *defaults.SkipFinalBackup != true {
		t.Errorf("default SkipFinalBackup was clobbered to false by an earlier parse")
	}
}
