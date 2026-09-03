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

package v1alpha1

import "fmt"

var (
	// SkipFinalBackupAnnotation controls whether FSx takes a final backup when a
	// FileSystem is deleted. Absent means skip, because a final backup outlives
	// the file system as an FSx Backup this controller does not model and cannot
	// reclaim. (FSx's own defaults differ per type: Lustre skips, Windows and
	// OpenZFS do not.)
	//
	// Set to "false" to opt in; that needs fsx:CreateBackup, which
	// AmazonFSxFullAccess already grants. No effect on ONTAP, which has no
	// delete configuration shape.
	SkipFinalBackupAnnotation = fmt.Sprintf("%s/skip-final-backup", GroupVersion.Group)

	// CascadeDeleteAnnotation opts an OpenZFS FileSystem into
	// DELETE_CHILD_VOLUMES_AND_SNAPSHOTS on delete. Absent means off, so FSx's
	// refusal to delete a file system with child volumes surfaces as a condition
	// instead: those children may be managed outside this controller. Set to
	// "true" to delete them along with the file system.
	//
	// No effect on other file system types.
	CascadeDeleteAnnotation = fmt.Sprintf("%s/cascade-delete", GroupVersion.Group)
)
