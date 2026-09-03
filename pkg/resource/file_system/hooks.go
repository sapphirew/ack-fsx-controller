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

package file_system

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
	"github.com/aws-controllers-k8s/fsx-controller/pkg/util"
)

// restoreNonRoundTrippedSpecFields copies Spec members DescribeFileSystems
// cannot round-trip back onto the resource built from an FSx response.
//
// sdkCreate and sdkFind assign ko.Spec.<Type>Configuration wholesale from the
// output shape, so members that shape omits are lost and the reconciler then
// erases them from the CR.
//
// Only write-only or create-only-and-unobservable members are restored.
// Anything Describe does report is left as the observed value: overwriting it
// with the desired value would make a user edit compare equal to AWS and
// suppress the update.
func restoreNonRoundTrippedSpecFields(desired *resource, ko *svcapitypes.FileSystem) {
	if desired == nil || desired.ko == nil || ko == nil {
		return
	}
	src := desired.ko.Spec

	// Nothing from LustreConfiguration is restored: every member Describe fails
	// to name-match is recovered with its OBSERVED value by
	// recoverObservedLustreDataRepository instead, so comparison stays honest.
	// Create-only: the output shape carries RootVolumeId instead.
	if src.OpenZFSConfiguration != nil && ko.Spec.OpenZFSConfiguration != nil {
		ko.Spec.OpenZFSConfiguration.RootVolumeConfiguration = src.OpenZFSConfiguration.RootVolumeConfiguration
	}

	if src.WindowsConfiguration != nil && ko.Spec.WindowsConfiguration != nil {
		// Immutable until Associate/DisassociateFileSystemAliases is wired up.
		ko.Spec.WindowsConfiguration.Aliases = src.WindowsConfiguration.Aliases

		// Only the write-only password. The rest of the AD block is reported by
		// Describe as SelfManagedActiveDirectoryAttributes and is updatable, so
		// replacing the whole struct would mask every edit to those fields.
		//
		// If Describe returned no AD block the reference is left out rather than
		// fabricating a configuration the user never submitted.
		if src.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration != nil &&
			ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration != nil {
			ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password =
				src.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password
		}
	}

	// Write-only: never returned by Describe.
	if src.OntapConfiguration != nil && ko.Spec.OntapConfiguration != nil {
		ko.Spec.OntapConfiguration.FsxAdminPassword = src.OntapConfiguration.FsxAdminPassword
	}
}

// immutableField pairs an immutable Spec path with a fingerprint of its value.
// The set mirrors every `is_immutable` entry in generator.yaml; a path present
// there but missing here loses change detection silently, so keep them in sync.
//
// fingerprint returns nil when the field is absent. Comparing fingerprints
// rather than presence alone matters for fields DescribeFileSystems does not
// return -- SecurityGroupIDs above all, whose resolved values ResolveReferences
// writes into the Spec, so desired and latest always agree and the generated
// delta is empty however the value changes.
type immutableField struct {
	path        string
	fingerprint func(*svcapitypes.FileSystem) *string
}

func fpJSON(v interface{}) *string {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return aws.String(string(encoded))
}

func fpStr(v *string) *string {
	if v == nil {
		return nil
	}
	return fpJSON(*v)
}

func fpBool(v *bool) *string {
	if v == nil {
		return nil
	}
	return fpJSON(*v)
}

func fpInt(v *int64) *string {
	if v == nil {
		return nil
	}
	return fpJSON(*v)
}

// fpStrSlice fingerprints a string slice order-insensitively: FSx does not
// preserve the order of security group or alias lists, so a reorder is not a
// change.
func fpStrSlice(v []*string) *string {
	if len(v) == 0 {
		return nil
	}
	out := make([]string, 0, len(v))
	for _, s := range v {
		if s != nil {
			out = append(out, *s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return fpJSON(out)
}

func fpStruct(v interface{}, present bool) *string {
	if !present {
		return nil
	}
	return fpJSON(v)
}

var immutableFields = []immutableField{
	{"Spec.FileSystemType", func(k *svcapitypes.FileSystem) *string { return fpStr(k.Spec.FileSystemType) }},
	{"Spec.SubnetIDs", func(k *svcapitypes.FileSystem) *string { return fpStrSlice(k.Spec.SubnetIDs) }},
	{"Spec.SecurityGroupIDs", func(k *svcapitypes.FileSystem) *string { return fpStrSlice(k.Spec.SecurityGroupIDs) }},
	{"Spec.KMSKeyID", func(k *svcapitypes.FileSystem) *string { return fpStr(k.Spec.KMSKeyID) }},

	{"Spec.LustreConfiguration.CopyTagsToBackups", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.LustreConfiguration == nil {
			return nil
		}
		return fpBool(k.Spec.LustreConfiguration.CopyTagsToBackups)
	}},
	{"Spec.LustreConfiguration.DeploymentType", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.LustreConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.LustreConfiguration.DeploymentType)
	}},
	{"Spec.LustreConfiguration.DriveCacheType", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.LustreConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.LustreConfiguration.DriveCacheType)
	}},
	{"Spec.LustreConfiguration.EfaEnabled", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.LustreConfiguration == nil {
			return nil
		}
		return fpBool(k.Spec.LustreConfiguration.EfaEnabled)
	}},
	{"Spec.LustreConfiguration.ExportPath", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.LustreConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.LustreConfiguration.ExportPath)
	}},
	{"Spec.LustreConfiguration.ImportPath", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.LustreConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.LustreConfiguration.ImportPath)
	}},
	{"Spec.LustreConfiguration.ImportedFileChunkSize", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.LustreConfiguration == nil {
			return nil
		}
		return fpInt(k.Spec.LustreConfiguration.ImportedFileChunkSize)
	}},

	{"Spec.OntapConfiguration.DeploymentType", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.OntapConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.OntapConfiguration.DeploymentType)
	}},
	{"Spec.OntapConfiguration.EndpointIPAddressRange", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.OntapConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.OntapConfiguration.EndpointIPAddressRange)
	}},
	{"Spec.OntapConfiguration.PreferredSubnetID", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.OntapConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.OntapConfiguration.PreferredSubnetID)
	}},

	{"Spec.OpenZFSConfiguration.DeploymentType", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.OpenZFSConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.OpenZFSConfiguration.DeploymentType)
	}},
	{"Spec.OpenZFSConfiguration.EndpointIPAddressRange", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.OpenZFSConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.OpenZFSConfiguration.EndpointIPAddressRange)
	}},
	{"Spec.OpenZFSConfiguration.PreferredSubnetID", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.OpenZFSConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.OpenZFSConfiguration.PreferredSubnetID)
	}},
	{"Spec.OpenZFSConfiguration.RootVolumeConfiguration", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.OpenZFSConfiguration == nil {
			return nil
		}
		return fpStruct(
			k.Spec.OpenZFSConfiguration.RootVolumeConfiguration,
			k.Spec.OpenZFSConfiguration.RootVolumeConfiguration != nil,
		)
	}},

	{"Spec.WindowsConfiguration.ActiveDirectoryID", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.WindowsConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.WindowsConfiguration.ActiveDirectoryID)
	}},
	{"Spec.WindowsConfiguration.Aliases", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.WindowsConfiguration == nil {
			return nil
		}
		return fpStrSlice(k.Spec.WindowsConfiguration.Aliases)
	}},
	{"Spec.WindowsConfiguration.CopyTagsToBackups", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.WindowsConfiguration == nil {
			return nil
		}
		return fpBool(k.Spec.WindowsConfiguration.CopyTagsToBackups)
	}},
	{"Spec.WindowsConfiguration.DeploymentType", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.WindowsConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.WindowsConfiguration.DeploymentType)
	}},
	{"Spec.WindowsConfiguration.PreferredSubnetID", func(k *svcapitypes.FileSystem) *string {
		if k.Spec.WindowsConfiguration == nil {
			return nil
		}
		return fpStr(k.Spec.WindowsConfiguration.PreferredSubnetID)
	}},
}

// declaredImmutableFingerprints maps each present immutable path to a
// fingerprint of its value. Absent paths are omitted.
func declaredImmutableFingerprints(ko *svcapitypes.FileSystem) map[string]string {
	if ko == nil {
		return nil
	}
	out := make(map[string]string, len(immutableFields))
	for _, f := range immutableFields {
		if fp := f.fingerprint(ko); fp != nil {
			out[f.path] = *fp
		}
	}
	return out
}

// setLastAppliedImmutableFields records the immutable values sent to FSx.
func setLastAppliedImmutableFields(ko *svcapitypes.FileSystem) {
	if ko == nil {
		return
	}
	declared := declaredImmutableFingerprints(ko)
	if declared == nil {
		declared = map[string]string{}
	}
	if encoded := fpJSON(declared); encoded != nil {
		ko.Status.LastAppliedImmutableFields = encoded
	}
}

// appliedImmutableFields decodes the recorded baseline. The second return value
// is false when nothing usable has been recorded, which must not be confused
// with a recorded empty map.
func appliedImmutableFields(ko *svcapitypes.FileSystem) (map[string]string, bool) {
	if ko == nil || ko.Status.LastAppliedImmutableFields == nil {
		return nil, false
	}
	var fps map[string]string
	if err := json.Unmarshal([]byte(*ko.Status.LastAppliedImmutableFields), &fps); err != nil {
		return nil, false
	}
	return fps, true
}

// ensureImmutableFieldBaseline records the baseline when none is usable yet, so
// adopted resources gain change detection without being rejected on their first
// reconcile. A malformed baseline counts as unusable and is re-recorded;
// a usable one is never overwritten, which would adopt the new values as though
// they had always been there.
func ensureImmutableFieldBaseline(ko *svcapitypes.FileSystem) {
	if ko == nil {
		return
	}
	if _, recorded := appliedImmutableFields(ko); recorded {
		return
	}
	setLastAppliedImmutableFields(ko)
}

// rejectImmutableFieldChanges returns a terminal error when an immutable field
// changed value or moved between absent and present.
//
// is_immutable is not sufficient alone: it emits a field-level
// `self == oldSelf` transition rule, which Kubernetes skips when the old value
// is absent, so absent->present passes admission. And the generated delta sees
// nothing for any field DescribeFileSystems does not return, in either
// direction, so the recorded create-time fingerprints are the only witness.
func rejectImmutableFieldChanges(
	delta *ackcompare.Delta,
	desired *resource,
	latest *resource,
) error {
	if desired == nil || desired.ko == nil {
		return nil
	}

	if delta != nil {
		for _, f := range immutableFields {
			if delta.DifferentAt(f.path) {
				return ackerr.NewTerminalError(fmt.Errorf(
					"%s is immutable and cannot be changed after creation", f.path,
				))
			}
		}
	}

	if latest == nil {
		return nil
	}
	applied, recorded := appliedImmutableFields(latest.ko)
	if !recorded {
		// No usable baseline yet (adopted, or malformed).
		// ensureImmutableFieldBaseline records one on this same read.
		return nil
	}
	declared := declaredImmutableFingerprints(desired.ko)
	for path, fp := range declared {
		prev, ok := applied[path]
		if !ok {
			return ackerr.NewTerminalError(fmt.Errorf(
				"%s is immutable and cannot be added after creation", path,
			))
		}
		if prev != fp {
			return ackerr.NewTerminalError(fmt.Errorf(
				"%s is immutable and cannot be changed after creation", path,
			))
		}
	}
	for path := range applied {
		if _, ok := declared[path]; !ok {
			return ackerr.NewTerminalError(fmt.Errorf(
				"%s is immutable and cannot be removed after creation", path,
			))
		}
	}
	return nil
}

// observedLustreDataRepository returns the DataRepositoryConfiguration FSx
// reports for the file system in ko, or nil.
//
// The Spec is generated from CreateFileSystemLustreConfiguration, which carries
// these members flat; Describe nests them under
// LustreFileSystemConfiguration.DataRepositoryConfiguration, which code-gen
// cannot name-match, so sdkFind leaves them nil.
func observedLustreDataRepository(
	resp *svcsdk.DescribeFileSystemsOutput,
	ko *svcapitypes.FileSystem,
) *svcsdktypes.DataRepositoryConfiguration {
	if resp == nil || ko == nil || ko.Status.FileSystemID == nil {
		return nil
	}
	for _, fs := range resp.FileSystems {
		if fs.FileSystemId == nil || *fs.FileSystemId != *ko.Status.FileSystemID {
			continue
		}
		if fs.LustreConfiguration == nil {
			return nil
		}
		return fs.LustreConfiguration.DataRepositoryConfiguration
	}
	return nil
}

// restoreLustreDataRepositoryFields copies the four Lustre data-repository
// members from the submitted resource. Used on the create path only.
//
// The CreateFileSystem response carries LustreFileSystemConfiguration, which
// nests these under DataRepositoryConfiguration and so cannot be name-matched
// into the flat Spec members. Without this they would be nil in the object the
// reconciler patches back, which both erases the user's declared values from the
// CR and records them as absent in the immutable baseline. On reads
// recoverObservedLustreDataRepository supplies the observed values instead, so
// comparison stays honest.
func restoreLustreDataRepositoryFields(desired *resource, ko *svcapitypes.FileSystem) {
	if desired == nil || desired.ko == nil || ko == nil {
		return
	}
	src := desired.ko.Spec.LustreConfiguration
	if src == nil || ko.Spec.LustreConfiguration == nil {
		return
	}
	ko.Spec.LustreConfiguration.AutoImportPolicy = src.AutoImportPolicy
	ko.Spec.LustreConfiguration.ExportPath = src.ExportPath
	ko.Spec.LustreConfiguration.ImportPath = src.ImportPath
	ko.Spec.LustreConfiguration.ImportedFileChunkSize = src.ImportedFileChunkSize
}

// recoverObservedLustreDataRepository copies the observed AutoImportPolicy,
// ImportPath, ExportPath and ImportedFileChunkSize into the Spec so the
// generated comparison sees real AWS state.
//
// Recovered only where the user declared the member: writing an AWS-chosen
// default into a Spec left empty would manufacture a delta and an
// UpdateFileSystem call nobody asked for.
func recoverObservedLustreDataRepository(
	desired *resource,
	resp *svcsdk.DescribeFileSystemsOutput,
	ko *svcapitypes.FileSystem,
) {
	if desired == nil || desired.ko == nil || ko == nil {
		return
	}
	src := desired.ko.Spec.LustreConfiguration
	if src == nil || ko.Spec.LustreConfiguration == nil {
		return
	}
	drc := observedLustreDataRepository(resp, ko)
	if drc == nil {
		return
	}
	if src.AutoImportPolicy != nil && drc.AutoImportPolicy != "" {
		ko.Spec.LustreConfiguration.AutoImportPolicy = aws.String(string(drc.AutoImportPolicy))
	}
	if src.ImportPath != nil && drc.ImportPath != nil {
		ko.Spec.LustreConfiguration.ImportPath = drc.ImportPath
	}
	if src.ExportPath != nil && drc.ExportPath != nil {
		ko.Spec.LustreConfiguration.ExportPath = drc.ExportPath
	}
	if src.ImportedFileChunkSize != nil && drc.ImportedFileChunkSize != nil {
		ko.Spec.LustreConfiguration.ImportedFileChunkSize = aws.Int64(
			int64(*drc.ImportedFileChunkSize),
		)
	}
}

// secretReferenceString renders a SecretKeyReference as a comparable string.
//
// JSON rather than a delimited format: dots are legal in both Secret names and
// keys, so "<ns>/<name>.<key>" is not injective -- {name: creds, key:
// admin.password} and {name: creds.admin, key: password} both render
// "default/creds.admin.password", so a change between them would read as no
// change at all. JSON quotes and separates each component.
func secretReferenceString(ref *ackv1alpha1.SecretKeyReference) string {
	if ref == nil {
		return ""
	}
	encoded, err := json.Marshal(struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
		Key       string `json:"key"`
	}{Namespace: ref.Namespace, Name: ref.Name, Key: ref.Key})
	if err != nil {
		// Cannot fail for a struct of plain strings, but fall back to a form
		// that is still unambiguous rather than silently returning "".
		return fmt.Sprintf("%q/%q.%q", ref.Namespace, ref.Name, ref.Key)
	}
	return string(encoded)
}

// fsxAdminPasswordRef and selfManagedADPasswordRef read the two secret-backed
// Spec fields, tolerating absent parent configuration blocks.
func fsxAdminPasswordRef(ko *svcapitypes.FileSystem) *ackv1alpha1.SecretKeyReference {
	if ko == nil || ko.Spec.OntapConfiguration == nil {
		return nil
	}
	return ko.Spec.OntapConfiguration.FsxAdminPassword
}

func selfManagedADPasswordRef(ko *svcapitypes.FileSystem) *ackv1alpha1.SecretKeyReference {
	if ko == nil || ko.Spec.WindowsConfiguration == nil ||
		ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration == nil {
		return nil
	}
	return ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password
}

// pathString renders a delta path. compare.Path keeps its parts unexported and
// exposes them only through MarshalJSON, so that is the supported way to read
// one back out.
func pathString(p ackcompare.Path) string {
	encoded, err := p.MarshalJSON()
	if err != nil {
		return ""
	}
	var parts struct{ Parts []string }
	if err := json.Unmarshal(encoded, &parts); err != nil {
		return ""
	}
	return strings.Join(parts.Parts, ".")
}

// missingRequiredMembers reports members the FSx model marks required that the
// built request leaves unset.
//
// The three shapes below are the complete set of required members reachable from
// UpdateFileSystemRequest (aside from FileSystemId, which the builder always
// sets). Absence is meaningful for plenty of other members -- removing every
// route table is expressed by an empty list, and the Lustre metadata and
// read-cache mode transitions require their sibling IOPS/SizeGiB to be omitted
// -- so only genuinely required members are checked.
func missingRequiredMembers(input *svcsdk.UpdateFileSystemInput) []string {
	if input == nil {
		return nil
	}
	missing := []string{}
	if lc := input.LustreConfiguration; lc != nil && lc.LogConfiguration != nil {
		if lc.LogConfiguration.Level == "" {
			missing = append(missing, "LustreConfiguration.LogConfiguration.Level")
		}
	}
	if wc := input.WindowsConfiguration; wc != nil {
		if al := wc.AuditLogConfiguration; al != nil {
			if al.FileAccessAuditLogLevel == "" {
				missing = append(missing,
					"WindowsConfiguration.AuditLogConfiguration.FileAccessAuditLogLevel")
			}
			if al.FileShareAccessAuditLogLevel == "" {
				missing = append(missing,
					"WindowsConfiguration.AuditLogConfiguration.FileShareAccessAuditLogLevel")
			}
		}
		if fs := wc.FsrmConfiguration; fs != nil && fs.FsrmServiceEnabled == nil {
			missing = append(missing,
				"WindowsConfiguration.FsrmConfiguration.FsrmServiceEnabled")
		}
	}
	return missing
}

// rejectUnexpressibleChanges returns a terminal error for any differing Spec
// path UpdateFileSystem cannot carry, before the caller applies side effects.
//
// The payload builders are the oracle: a path is expressible exactly when
// building a request from a delta containing only that path yields something.
// A removal fails that test because the builders only assign members whose
// desired value is non-nil, and so does a field with no builder at all. Using
// the builders rather than a hand-maintained list keeps the two from drifting.
func (rm *resourceManager) rejectUnexpressibleChanges(
	ctx context.Context,
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) error {
	paths := deltaPathsOutsideTags(delta)
	if len(paths) == 0 {
		return nil
	}
	unexpressible := []string{}
	invalid := []string{}
	for _, path := range paths {
		single := ackcompare.NewDelta()
		single.Add(path, nil, nil)
		input, err := rm.newUpdateRequestPayload(ctx, desired, latest, single)
		if err != nil {
			// Surfacing this here rather than after side effects is the point:
			// an int32 overflow or an empty password Secret should stop the
			// update before tags are touched.
			return err
		}
		if input == nil {
			unexpressible = append(unexpressible, path)
			continue
		}
		// A non-nil payload is not enough. DifferentAt matches ancestors, so
		// probing a nested leaf activates the branch for its enclosing struct
		// and the builder rebuilds that struct from whichever members survive.
		// When the removed leaf was required by the shape, the result is a
		// request FSx rejects -- so validate rather than trust the payload.
		if missing := missingRequiredMembers(input); len(missing) > 0 {
			invalid = append(invalid, fmt.Sprintf("%s (%s)", path, strings.Join(missing, ", ")))
		}
	}
	if len(invalid) > 0 {
		return ackerr.NewTerminalError(fmt.Errorf(
			"cannot apply changes to %s: the request FSx would receive is missing "+
				"members the API requires, so set them explicitly rather than removing them",
			strings.Join(invalid, "; "),
		))
	}
	if len(unexpressible) == 0 {
		return nil
	}
	return ackerr.NewTerminalError(fmt.Errorf(
		"cannot apply changes to %s: UpdateFileSystem has no parameter for this "+
			"change (removing a value is not supported; set it explicitly instead)",
		strings.Join(unexpressible, ", "),
	))
}

// deltaPathsOutsideTags returns the differing Spec paths that are not tags.
func deltaPathsOutsideTags(delta *ackcompare.Delta) []string {
	if delta == nil || !delta.DifferentExcept("Spec.Tags") {
		return nil
	}
	seen := map[string]bool{}
	out := []string{}
	for _, d := range delta.Differences {
		p := pathString(d.Path)
		if p == "" || strings.HasPrefix(p, "Spec.Tags") {
			continue
		}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// carriedPasswords records which secret-backed members a request actually
// carried, so only those baselines advance.
type carriedPasswords struct {
	fsxAdmin bool
	smad     bool
}

func requestCarriedPasswords(input *svcsdk.UpdateFileSystemInput) carriedPasswords {
	var c carriedPasswords
	if input == nil {
		return c
	}
	if input.OntapConfiguration != nil && input.OntapConfiguration.FsxAdminPassword != nil {
		c.fsxAdmin = true
	}
	if input.WindowsConfiguration != nil &&
		input.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration != nil &&
		input.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password != nil {
		c.smad = true
	}
	return c
}

// advanceLastAppliedSecretRefs records the references whose password was
// included in a successful request.
func advanceLastAppliedSecretRefs(ko *svcapitypes.FileSystem, carried carriedPasswords) {
	if ko == nil {
		return
	}
	if carried.fsxAdmin {
		ko.Status.LastAppliedFsxAdminPasswordRef = aws.String(
			secretReferenceString(fsxAdminPasswordRef(ko)),
		)
	}
	if carried.smad {
		ko.Status.LastAppliedSelfManagedADPasswordRef = aws.String(
			secretReferenceString(selfManagedADPasswordRef(ko)),
		)
	}
}

// errEmptySecret reports a referenced Secret key holding an empty value.
//
// Recoverable, not terminal: the Secret is usually populated moments later by an
// external process. Omitting the password instead would let creation succeed
// without it and record the reference as applied, after which filling the same
// Secret would never be detected -- comparison tracks the reference, not the
// value.
func errEmptySecret(field string, ref *ackv1alpha1.SecretKeyReference) error {
	return ackrequeue.NeededAfter(fmt.Errorf(
		"%s references Secret %s which holds an empty value",
		field, secretReferenceString(ref),
	), 15*time.Second)
}

// setLastAppliedSecretRefs records the secret references just sent to FSx, so a
// later change to either is detectable.
func setLastAppliedSecretRefs(ko *svcapitypes.FileSystem) {
	if ko == nil {
		return
	}
	ko.Status.LastAppliedFsxAdminPasswordRef = aws.String(
		secretReferenceString(fsxAdminPasswordRef(ko)),
	)
	ko.Status.LastAppliedSelfManagedADPasswordRef = aws.String(
		secretReferenceString(selfManagedADPasswordRef(ko)),
	)
}

// ensureLastAppliedSecretBaseline records the current secret references as the
// applied baseline when none exists yet, without triggering an update. Adopted
// resources never went through sdkCreate, so without this customPreCompare has
// nothing to compare against and rotation is permanently impossible.
//
// The baseline lives in Status because HandleReconcilerError patches Status on
// every reconcile, whereas patchResourceMetadataAndSpec does not run on an idle
// reconcile and setResourceManagedAndAdopted takes its patch base from the
// already-mutated `latest`, so an annotation set here would be diffed away.
//
// An existing baseline is never overwritten: that would adopt the newly
// requested reference as already applied and mask the rotation.
func ensureLastAppliedSecretBaseline(ko *svcapitypes.FileSystem) {
	if ko == nil {
		return
	}
	if ko.Status.LastAppliedFsxAdminPasswordRef == nil {
		ko.Status.LastAppliedFsxAdminPasswordRef = aws.String(
			secretReferenceString(fsxAdminPasswordRef(ko)),
		)
	}
	if ko.Status.LastAppliedSelfManagedADPasswordRef == nil {
		ko.Status.LastAppliedSelfManagedADPasswordRef = aws.String(
			secretReferenceString(selfManagedADPasswordRef(ko)),
		)
	}
}

// customPreCompare injects deltas the generated comparison cannot produce.
//
// Both password fields are compare-ignored (Describe never returns them), so
// without this a rotation would be accepted and never applied. The baseline
// comes from `b`, the read-back resource carrying the persisted Status.
func customPreCompare(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	if a == nil || a.ko == nil || b == nil || b.ko == nil {
		return
	}

	for _, c := range []struct {
		path     string
		desired  *ackv1alpha1.SecretKeyReference
		baseline *string
	}{
		{
			path:     "Spec.OntapConfiguration.FsxAdminPassword",
			desired:  fsxAdminPasswordRef(a.ko),
			baseline: b.ko.Status.LastAppliedFsxAdminPasswordRef,
		},
		{
			path:     "Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password",
			desired:  selfManagedADPasswordRef(a.ko),
			baseline: b.ko.Status.LastAppliedSelfManagedADPasswordRef,
		},
	} {
		desiredRef := secretReferenceString(c.desired)
		if desiredRef == "" {
			// No reference to apply; nothing to rotate.
			continue
		}
		if c.baseline == nil {
			// No baseline yet (adopted, or first reconcile). Treat the current
			// reference as already applied rather than resetting a password the
			// user did not touch; ensureLastAppliedSecretBaseline records it on
			// this same read so a subsequent change is detected.
			continue
		}
		if *c.baseline != desiredRef {
			delta.Add(c.path, *c.baseline, desiredRef)
		}
	}
}

// setDeleteFileSystemConfiguration fills in DeleteFileSystemInput's per-type
// Delete*Configuration members, which generator.yaml strips from the model but
// which still exist on the SDK struct. Both options come from annotations -- see
// svcapitypes.SkipFinalBackupAnnotation and CascadeDeleteAnnotation.
//
// ONTAP has no DeleteFileSystemOntapConfiguration shape.
func setDeleteFileSystemConfiguration(
	r *resource,
	input *svcsdk.DeleteFileSystemInput,
) error {
	if r.ko.Spec.FileSystemType == nil {
		return nil
	}
	params, err := util.ParseDeletionAnnotations(r.ko.ObjectMeta.GetAnnotations())
	if err != nil {
		// Retrying cannot fix a malformed annotation value. Surface it as
		// terminal so the condition message tells the user what to correct;
		// editing the annotation triggers a fresh reconcile.
		return ackerr.NewTerminalError(err)
	}
	switch svcsdktypes.FileSystemType(*r.ko.Spec.FileSystemType) {
	case svcsdktypes.FileSystemTypeLustre:
		input.LustreConfiguration = &svcsdktypes.DeleteFileSystemLustreConfiguration{
			SkipFinalBackup: params.SkipFinalBackup,
		}
	case svcsdktypes.FileSystemTypeOpenzfs:
		cfg := &svcsdktypes.DeleteFileSystemOpenZFSConfiguration{
			SkipFinalBackup: params.SkipFinalBackup,
		}
		// Opt-in only. DELETE_CHILD_VOLUMES_AND_SNAPSHOTS destroys every child
		// volume and snapshot, which may be managed outside this controller;
		// ignoring the Volume/Snapshot CRDs does not establish ownership. By
		// default let FSx reject the delete so the dependency surfaces as a
		// condition the user can act on.
		if params.CascadeDelete != nil && *params.CascadeDelete {
			cfg.Options = []svcsdktypes.DeleteFileSystemOpenZFSOption{
				svcsdktypes.DeleteFileSystemOpenZFSOptionDeleteChildVolumesAndSnapshots,
			}
		}
		input.OpenZFSConfiguration = cfg
	case svcsdktypes.FileSystemTypeWindows:
		input.WindowsConfiguration = &svcsdktypes.DeleteFileSystemWindowsConfiguration{
			SkipFinalBackup: params.SkipFinalBackup,
		}
	}
	return nil
}

// requeueWaitWhileDeleting is returned from the delete path while FSx still
// reports the file system. DeleteFileSystem is asynchronous and a file system
// stays in the DELETING lifecycle state for 5-10 minutes, during which its
// network interfaces are still attached to the subnet.
var requeueWaitWhileDeleting = ackrequeue.NeededAfter(
	fmt.Errorf(
		"file system is in %q state, requeuing until it is fully deleted",
		svcsdktypes.FileSystemLifecycleDeleting,
	),
	30*time.Second,
)

// isDeleting returns true if FSx has already accepted a delete request for the
// supplied file system.
func isDeleting(r *resource) bool {
	if r == nil || r.ko.Status.Lifecycle == nil {
		return false
	}
	return *r.ko.Status.Lifecycle == string(svcsdktypes.FileSystemLifecycleDeleting)
}

// int32Converter narrows the *int64 fields the CRD exposes (Kubernetes API
// convention) down to the *int32 fields the FSx SDK expects. Values that do
// not fit are a permanent user error, so the first failure is recorded as a
// terminal error and every subsequent conversion short-circuits.
type int32Converter struct {
	err error
}

func (c *int32Converter) convert(fieldPath string, v *int64) *int32 {
	if c.err != nil || v == nil {
		return nil
	}
	if *v > math.MaxInt32 || *v < math.MinInt32 {
		c.err = ackerr.NewTerminalError(fmt.Errorf(
			"%s must fit within an int32, got %d", fieldPath, *v,
		))
		return nil
	}
	out := int32(*v)
	return &out
}

// routeTableIDsDelta returns the route table IDs to add and remove to move
// from `latest` to `desired`. Create takes one RouteTableIds list (what the
// Spec exposes); Update takes AddRouteTableIds/RemoveRouteTableIds.
func routeTableIDsDelta(desired, latest []*string) (add, remove []string) {
	desiredSet := make(map[string]struct{}, len(desired))
	for _, id := range desired {
		if id != nil {
			desiredSet[*id] = struct{}{}
		}
	}
	latestSet := make(map[string]struct{}, len(latest))
	for _, id := range latest {
		if id != nil {
			latestSet[*id] = struct{}{}
		}
	}

	for _, id := range desired {
		if id == nil {
			continue
		}
		if _, found := latestSet[*id]; !found {
			add = append(add, *id)
		}
	}
	for _, id := range latest {
		if id == nil {
			continue
		}
		if _, found := desiredSet[*id]; !found {
			remove = append(remove, *id)
		}
	}
	return add, remove
}

// newDiskIOPSConfiguration converts the Spec representation of a disk IOPS
// configuration into the SDK shape shared by the ONTAP, OpenZFS and Windows
// update configurations.
func newDiskIOPSConfiguration(
	spec *svcapitypes.DiskIOPSConfiguration,
) *svcsdktypes.DiskIopsConfiguration {
	if spec == nil {
		return nil
	}
	res := &svcsdktypes.DiskIopsConfiguration{}
	if spec.IOPS != nil {
		res.Iops = spec.IOPS
	}
	if spec.Mode != nil {
		res.Mode = svcsdktypes.DiskIopsConfigurationMode(*spec.Mode)
	}
	return res
}

// newUpdateFileSystemLustreConfiguration builds the Lustre half of an
// UpdateFileSystem request, populating only members the delta reports as
// changed: UpdateFileSystem applies exactly what it is sent, and several
// Lustre properties are rejected on deployment types that lack them.
// Returns nil when nothing Lustre-related changed.
func newUpdateFileSystemLustreConfiguration(
	delta *ackcompare.Delta,
	spec *svcapitypes.CreateFileSystemLustreConfiguration,
	conv *int32Converter,
) *svcsdktypes.UpdateFileSystemLustreConfiguration {
	if spec == nil {
		return nil
	}
	const p = "Spec.LustreConfiguration."
	res := &svcsdktypes.UpdateFileSystemLustreConfiguration{}
	changed := false

	// Unreachable today: AutoImportPolicy is compare-ignored because Describe
	// returns it under DataRepositoryConfiguration, which code-gen cannot
	// round-trip. Kept correct in case a read hook ever recovers the value.
	if delta.DifferentAt(p+"AutoImportPolicy") && spec.AutoImportPolicy != nil {
		res.AutoImportPolicy = svcsdktypes.AutoImportPolicyType(*spec.AutoImportPolicy)
		changed = true
	}
	if delta.DifferentAt(p+"AutomaticBackupRetentionDays") && spec.AutomaticBackupRetentionDays != nil {
		res.AutomaticBackupRetentionDays = conv.convert(
			p+"AutomaticBackupRetentionDays", spec.AutomaticBackupRetentionDays,
		)
		changed = true
	}
	if delta.DifferentAt(p+"DailyAutomaticBackupStartTime") && spec.DailyAutomaticBackupStartTime != nil {
		res.DailyAutomaticBackupStartTime = spec.DailyAutomaticBackupStartTime
		changed = true
	}
	if delta.DifferentAt(p+"DataCompressionType") && spec.DataCompressionType != nil {
		res.DataCompressionType = svcsdktypes.DataCompressionType(*spec.DataCompressionType)
		changed = true
	}
	if delta.DifferentAt(p+"DataReadCacheConfiguration") && spec.DataReadCacheConfiguration != nil {
		drcCfg := &svcsdktypes.LustreReadCacheConfiguration{}
		drcCfg.SizeGiB = conv.convert(
			p+"DataReadCacheConfiguration.SizeGiB", spec.DataReadCacheConfiguration.SizeGiB,
		)
		if spec.DataReadCacheConfiguration.SizingMode != nil {
			drcCfg.SizingMode = svcsdktypes.LustreReadCacheSizingMode(
				*spec.DataReadCacheConfiguration.SizingMode,
			)
		}
		res.DataReadCacheConfiguration = drcCfg
		changed = true
	}
	if delta.DifferentAt(p+"LogConfiguration") && spec.LogConfiguration != nil {
		logCfg := &svcsdktypes.LustreLogCreateConfiguration{}
		if spec.LogConfiguration.Destination != nil {
			logCfg.Destination = spec.LogConfiguration.Destination
		}
		if spec.LogConfiguration.Level != nil {
			logCfg.Level = svcsdktypes.LustreAccessAuditLogLevel(*spec.LogConfiguration.Level)
		}
		res.LogConfiguration = logCfg
		changed = true
	}
	if delta.DifferentAt(p+"MetadataConfiguration") && spec.MetadataConfiguration != nil {
		mdCfg := &svcsdktypes.UpdateFileSystemLustreMetadataConfiguration{}
		mdCfg.Iops = conv.convert(p+"MetadataConfiguration.IOPS", spec.MetadataConfiguration.IOPS)
		if spec.MetadataConfiguration.Mode != nil {
			mdCfg.Mode = svcsdktypes.MetadataConfigurationMode(*spec.MetadataConfiguration.Mode)
		}
		res.MetadataConfiguration = mdCfg
		changed = true
	}
	if delta.DifferentAt(p+"PerUnitStorageThroughput") && spec.PerUnitStorageThroughput != nil {
		res.PerUnitStorageThroughput = conv.convert(
			p+"PerUnitStorageThroughput", spec.PerUnitStorageThroughput,
		)
		changed = true
	}
	if delta.DifferentAt(p+"RootSquashConfiguration") && spec.RootSquashConfiguration != nil {
		rsCfg := &svcsdktypes.LustreRootSquashConfiguration{}
		if spec.RootSquashConfiguration.NoSquashNids != nil {
			rsCfg.NoSquashNids = aws.ToStringSlice(spec.RootSquashConfiguration.NoSquashNids)
		}
		if spec.RootSquashConfiguration.RootSquash != nil {
			rsCfg.RootSquash = spec.RootSquashConfiguration.RootSquash
		}
		res.RootSquashConfiguration = rsCfg
		changed = true
	}
	if delta.DifferentAt(p+"ThroughputCapacity") && spec.ThroughputCapacity != nil {
		res.ThroughputCapacity = conv.convert(p+"ThroughputCapacity", spec.ThroughputCapacity)
		changed = true
	}
	if delta.DifferentAt(p+"WeeklyMaintenanceStartTime") && spec.WeeklyMaintenanceStartTime != nil {
		res.WeeklyMaintenanceStartTime = spec.WeeklyMaintenanceStartTime
		changed = true
	}

	if !changed {
		return nil
	}
	return res
}

// newUpdateFileSystemOntapConfiguration builds the ONTAP half of an
// UpdateFileSystem request; see the Lustre variant for why only changed
// members are populated. Returns nil when nothing ONTAP-related changed.
func (rm *resourceManager) newUpdateFileSystemOntapConfiguration(
	ctx context.Context,
	delta *ackcompare.Delta,
	desired *resource,
	latest *resource,
	conv *int32Converter,
) (*svcsdktypes.UpdateFileSystemOntapConfiguration, error) {
	spec := desired.ko.Spec.OntapConfiguration
	if spec == nil {
		return nil, nil
	}
	const p = "Spec.OntapConfiguration."
	res := &svcsdktypes.UpdateFileSystemOntapConfiguration{}
	changed := false

	if delta.DifferentAt(p + "RouteTableIDs") {
		var latestRouteTableIDs []*string
		if latest != nil && latest.ko.Spec.OntapConfiguration != nil {
			latestRouteTableIDs = latest.ko.Spec.OntapConfiguration.RouteTableIDs
		}
		add, remove := routeTableIDsDelta(spec.RouteTableIDs, latestRouteTableIDs)
		if len(add) > 0 {
			res.AddRouteTableIds = add
			changed = true
		}
		if len(remove) > 0 {
			res.RemoveRouteTableIds = remove
			changed = true
		}
	}
	if delta.DifferentAt(p+"AutomaticBackupRetentionDays") && spec.AutomaticBackupRetentionDays != nil {
		res.AutomaticBackupRetentionDays = conv.convert(
			p+"AutomaticBackupRetentionDays", spec.AutomaticBackupRetentionDays,
		)
		changed = true
	}
	if delta.DifferentAt(p+"DailyAutomaticBackupStartTime") && spec.DailyAutomaticBackupStartTime != nil {
		res.DailyAutomaticBackupStartTime = spec.DailyAutomaticBackupStartTime
		changed = true
	}
	if delta.DifferentAt(p+"DiskIOPSConfiguration") && spec.DiskIOPSConfiguration != nil {
		res.DiskIopsConfiguration = newDiskIOPSConfiguration(spec.DiskIOPSConfiguration)
		changed = true
	}
	if delta.DifferentAt(p+"EndpointIPv6AddressRange") && spec.EndpointIPv6AddressRange != nil {
		res.EndpointIpv6AddressRange = spec.EndpointIPv6AddressRange
		changed = true
	}
	// FsxAdminPassword is compare-ignored (Describe never returns it), so this
	// path only appears when customPreCompare detects the Spec pointing at a
	// different Secret than the last-applied annotation records.
	if delta.DifferentAt(p+"FsxAdminPassword") && spec.FsxAdminPassword != nil {
		tmpSecret, err := rm.rr.SecretValueFromReference(ctx, spec.FsxAdminPassword)
		if err != nil {
			return nil, ackrequeue.Needed(err)
		}
		if tmpSecret == "" {
			return nil, errEmptySecret(p+"FsxAdminPassword", spec.FsxAdminPassword)
		}
		res.FsxAdminPassword = aws.String(tmpSecret)
		changed = true
	}
	if delta.DifferentAt(p+"HAPairs") && spec.HAPairs != nil {
		res.HAPairs = conv.convert(p+"HAPairs", spec.HAPairs)
		changed = true
	}
	if delta.DifferentAt(p+"ThroughputCapacity") && spec.ThroughputCapacity != nil {
		res.ThroughputCapacity = conv.convert(p+"ThroughputCapacity", spec.ThroughputCapacity)
		changed = true
	}
	if delta.DifferentAt(p+"ThroughputCapacityPerHAPair") && spec.ThroughputCapacityPerHAPair != nil {
		res.ThroughputCapacityPerHAPair = conv.convert(
			p+"ThroughputCapacityPerHAPair", spec.ThroughputCapacityPerHAPair,
		)
		changed = true
	}
	if delta.DifferentAt(p+"WeeklyMaintenanceStartTime") && spec.WeeklyMaintenanceStartTime != nil {
		res.WeeklyMaintenanceStartTime = spec.WeeklyMaintenanceStartTime
		changed = true
	}

	if !changed {
		return nil, nil
	}
	return res, nil
}

// newUpdateFileSystemOpenZFSConfiguration builds the OpenZFS half of an
// UpdateFileSystem request; see the Lustre variant for why only changed
// members are populated. Returns nil when nothing OpenZFS-related changed.
func newUpdateFileSystemOpenZFSConfiguration(
	delta *ackcompare.Delta,
	desired *resource,
	latest *resource,
	conv *int32Converter,
) *svcsdktypes.UpdateFileSystemOpenZFSConfiguration {
	spec := desired.ko.Spec.OpenZFSConfiguration
	if spec == nil {
		return nil
	}
	const p = "Spec.OpenZFSConfiguration."
	res := &svcsdktypes.UpdateFileSystemOpenZFSConfiguration{}
	changed := false

	if delta.DifferentAt(p + "RouteTableIDs") {
		var latestRouteTableIDs []*string
		if latest != nil && latest.ko.Spec.OpenZFSConfiguration != nil {
			latestRouteTableIDs = latest.ko.Spec.OpenZFSConfiguration.RouteTableIDs
		}
		add, remove := routeTableIDsDelta(spec.RouteTableIDs, latestRouteTableIDs)
		if len(add) > 0 {
			res.AddRouteTableIds = add
			changed = true
		}
		if len(remove) > 0 {
			res.RemoveRouteTableIds = remove
			changed = true
		}
	}
	if delta.DifferentAt(p+"AutomaticBackupRetentionDays") && spec.AutomaticBackupRetentionDays != nil {
		res.AutomaticBackupRetentionDays = conv.convert(
			p+"AutomaticBackupRetentionDays", spec.AutomaticBackupRetentionDays,
		)
		changed = true
	}
	if delta.DifferentAt(p+"CopyTagsToBackups") && spec.CopyTagsToBackups != nil {
		res.CopyTagsToBackups = spec.CopyTagsToBackups
		changed = true
	}
	if delta.DifferentAt(p+"CopyTagsToVolumes") && spec.CopyTagsToVolumes != nil {
		res.CopyTagsToVolumes = spec.CopyTagsToVolumes
		changed = true
	}
	if delta.DifferentAt(p+"DailyAutomaticBackupStartTime") && spec.DailyAutomaticBackupStartTime != nil {
		res.DailyAutomaticBackupStartTime = spec.DailyAutomaticBackupStartTime
		changed = true
	}
	if delta.DifferentAt(p+"DiskIOPSConfiguration") && spec.DiskIOPSConfiguration != nil {
		res.DiskIopsConfiguration = newDiskIOPSConfiguration(spec.DiskIOPSConfiguration)
		changed = true
	}
	if delta.DifferentAt(p+"EndpointIPv6AddressRange") && spec.EndpointIPv6AddressRange != nil {
		res.EndpointIpv6AddressRange = spec.EndpointIPv6AddressRange
		changed = true
	}
	if delta.DifferentAt(p+"ReadCacheConfiguration") && spec.ReadCacheConfiguration != nil {
		rcCfg := &svcsdktypes.OpenZFSReadCacheConfiguration{}
		rcCfg.SizeGiB = conv.convert(p+"ReadCacheConfiguration.SizeGiB", spec.ReadCacheConfiguration.SizeGiB)
		if spec.ReadCacheConfiguration.SizingMode != nil {
			rcCfg.SizingMode = svcsdktypes.OpenZFSReadCacheSizingMode(*spec.ReadCacheConfiguration.SizingMode)
		}
		res.ReadCacheConfiguration = rcCfg
		changed = true
	}
	if delta.DifferentAt(p+"ThroughputCapacity") && spec.ThroughputCapacity != nil {
		res.ThroughputCapacity = conv.convert(p+"ThroughputCapacity", spec.ThroughputCapacity)
		changed = true
	}
	if delta.DifferentAt(p+"WeeklyMaintenanceStartTime") && spec.WeeklyMaintenanceStartTime != nil {
		res.WeeklyMaintenanceStartTime = spec.WeeklyMaintenanceStartTime
		changed = true
	}

	if !changed {
		return nil
	}
	return res
}

// newUpdateFileSystemWindowsConfiguration builds the Windows half of an
// UpdateFileSystem request; see the Lustre variant for why only changed
// members are populated. Returns nil when nothing Windows-related changed.
//
// Aliases has no member here: FSx changes them via
// Associate/DisassociateFileSystemAliases.
func (rm *resourceManager) newUpdateFileSystemWindowsConfiguration(
	ctx context.Context,
	delta *ackcompare.Delta,
	desired *resource,
	conv *int32Converter,
) (*svcsdktypes.UpdateFileSystemWindowsConfiguration, error) {
	spec := desired.ko.Spec.WindowsConfiguration
	if spec == nil {
		return nil, nil
	}
	const p = "Spec.WindowsConfiguration."
	res := &svcsdktypes.UpdateFileSystemWindowsConfiguration{}
	changed := false

	if delta.DifferentAt(p+"AuditLogConfiguration") && spec.AuditLogConfiguration != nil {
		alCfg := &svcsdktypes.WindowsAuditLogCreateConfiguration{}
		if spec.AuditLogConfiguration.AuditLogDestination != nil {
			alCfg.AuditLogDestination = spec.AuditLogConfiguration.AuditLogDestination
		}
		if spec.AuditLogConfiguration.FileAccessAuditLogLevel != nil {
			alCfg.FileAccessAuditLogLevel = svcsdktypes.WindowsAccessAuditLogLevel(
				*spec.AuditLogConfiguration.FileAccessAuditLogLevel,
			)
		}
		if spec.AuditLogConfiguration.FileShareAccessAuditLogLevel != nil {
			alCfg.FileShareAccessAuditLogLevel = svcsdktypes.WindowsAccessAuditLogLevel(
				*spec.AuditLogConfiguration.FileShareAccessAuditLogLevel,
			)
		}
		res.AuditLogConfiguration = alCfg
		changed = true
	}
	if delta.DifferentAt(p+"AutomaticBackupRetentionDays") && spec.AutomaticBackupRetentionDays != nil {
		res.AutomaticBackupRetentionDays = conv.convert(
			p+"AutomaticBackupRetentionDays", spec.AutomaticBackupRetentionDays,
		)
		changed = true
	}
	if delta.DifferentAt(p+"DailyAutomaticBackupStartTime") && spec.DailyAutomaticBackupStartTime != nil {
		res.DailyAutomaticBackupStartTime = spec.DailyAutomaticBackupStartTime
		changed = true
	}
	if delta.DifferentAt(p+"DiskIOPSConfiguration") && spec.DiskIOPSConfiguration != nil {
		res.DiskIopsConfiguration = newDiskIOPSConfiguration(spec.DiskIOPSConfiguration)
		changed = true
	}
	if delta.DifferentAt(p+"FsrmConfiguration") && spec.FsrmConfiguration != nil {
		fsrmCfg := &svcsdktypes.WindowsFsrmConfiguration{}
		if spec.FsrmConfiguration.EventLogDestination != nil {
			fsrmCfg.EventLogDestination = spec.FsrmConfiguration.EventLogDestination
		}
		if spec.FsrmConfiguration.FsrmServiceEnabled != nil {
			fsrmCfg.FsrmServiceEnabled = spec.FsrmConfiguration.FsrmServiceEnabled
		}
		res.FsrmConfiguration = fsrmCfg
		changed = true
	}
	if delta.DifferentAt(p+"SelfManagedActiveDirectoryConfiguration") &&
		spec.SelfManagedActiveDirectoryConfiguration != nil {
		smad := spec.SelfManagedActiveDirectoryConfiguration
		smadUpdates := &svcsdktypes.SelfManagedActiveDirectoryConfigurationUpdates{}
		if smad.DNSIPs != nil {
			smadUpdates.DnsIps = aws.ToStringSlice(smad.DNSIPs)
		}
		if smad.DomainJoinServiceAccountSecret != nil {
			smadUpdates.DomainJoinServiceAccountSecret = smad.DomainJoinServiceAccountSecret
		}
		if smad.DomainName != nil {
			smadUpdates.DomainName = smad.DomainName
		}
		if smad.FileSystemAdministratorsGroup != nil {
			smadUpdates.FileSystemAdministratorsGroup = smad.FileSystemAdministratorsGroup
		}
		if smad.OrganizationalUnitDistinguishedName != nil {
			smadUpdates.OrganizationalUnitDistinguishedName = smad.OrganizationalUnitDistinguishedName
		}
		if smad.UserName != nil {
			smadUpdates.UserName = smad.UserName
		}
		// Reached either because another AD member changed or because
		// customPreCompare flagged a password rotation (DifferentAt matches
		// ancestor paths, so the injected .Password difference opens this
		// block). Always send a complete SelfManagedActiveDirectoryConfigurationUpdates.
		if smad.Password != nil {
			tmpSecret, err := rm.rr.SecretValueFromReference(ctx, smad.Password)
			if err != nil {
				return nil, ackrequeue.Needed(err)
			}
			if tmpSecret == "" {
				return nil, errEmptySecret(
					p+"SelfManagedActiveDirectoryConfiguration.Password", smad.Password,
				)
			}
			smadUpdates.Password = aws.String(tmpSecret)
		}
		res.SelfManagedActiveDirectoryConfiguration = smadUpdates
		changed = true
	}
	if delta.DifferentAt(p+"ThroughputCapacity") && spec.ThroughputCapacity != nil {
		res.ThroughputCapacity = conv.convert(p+"ThroughputCapacity", spec.ThroughputCapacity)
		changed = true
	}
	if delta.DifferentAt(p+"WeeklyMaintenanceStartTime") && spec.WeeklyMaintenanceStartTime != nil {
		res.WeeklyMaintenanceStartTime = spec.WeeklyMaintenanceStartTime
		changed = true
	}

	if !changed {
		return nil, nil
	}
	return res, nil
}

// newUpdateRequestPayload builds the UpdateFileSystem request for the supplied
// delta, returning (nil, nil) when there is nothing UpdateFileSystem can act
// on -- the caller must then skip the API call.
func (rm *resourceManager) newUpdateRequestPayload(
	ctx context.Context,
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) (*svcsdk.UpdateFileSystemInput, error) {
	conv := &int32Converter{}
	// Prefer the ID from the most recent read; fall back to the stored CR in
	// case `latest` was assembled without it.
	fileSystemID := latest.ko.Status.FileSystemID
	if fileSystemID == nil {
		fileSystemID = desired.ko.Status.FileSystemID
	}
	res := &svcsdk.UpdateFileSystemInput{
		FileSystemId: fileSystemID,
	}
	changed := false

	if delta.DifferentAt("Spec.StorageCapacity") && desired.ko.Spec.StorageCapacity != nil {
		res.StorageCapacity = conv.convert("Spec.StorageCapacity", desired.ko.Spec.StorageCapacity)
		changed = true
	}
	if delta.DifferentAt("Spec.StorageType") && desired.ko.Spec.StorageType != nil {
		res.StorageType = svcsdktypes.StorageType(*desired.ko.Spec.StorageType)
		changed = true
	}
	if delta.DifferentAt("Spec.FileSystemTypeVersion") && desired.ko.Spec.FileSystemTypeVersion != nil {
		res.FileSystemTypeVersion = desired.ko.Spec.FileSystemTypeVersion
		changed = true
	}
	if delta.DifferentAt("Spec.NetworkType") && desired.ko.Spec.NetworkType != nil {
		res.NetworkType = svcsdktypes.NetworkType(*desired.ko.Spec.NetworkType)
		changed = true
	}

	if lustreCfg := newUpdateFileSystemLustreConfiguration(
		delta, desired.ko.Spec.LustreConfiguration, conv,
	); lustreCfg != nil {
		res.LustreConfiguration = lustreCfg
		changed = true
	}

	ontapCfg, err := rm.newUpdateFileSystemOntapConfiguration(ctx, delta, desired, latest, conv)
	if err != nil {
		return nil, err
	}
	if ontapCfg != nil {
		res.OntapConfiguration = ontapCfg
		changed = true
	}

	if openZFSCfg := newUpdateFileSystemOpenZFSConfiguration(
		delta, desired, latest, conv,
	); openZFSCfg != nil {
		res.OpenZFSConfiguration = openZFSCfg
		changed = true
	}

	windowsCfg, err := rm.newUpdateFileSystemWindowsConfiguration(ctx, delta, desired, conv)
	if err != nil {
		return nil, err
	}
	if windowsCfg != nil {
		res.WindowsConfiguration = windowsCfg
		changed = true
	}

	if conv.err != nil {
		return nil, conv.err
	}
	if !changed {
		return nil, nil
	}
	return res, nil
}

// rejectEmptyResolvedPasswords fails the create when a declared password
// reference resolved to an empty value, which the generated builder silently
// omits from the request.
func rejectEmptyResolvedPasswords(
	r *resource,
	input *svcsdk.CreateFileSystemInput,
) error {
	if r == nil || r.ko == nil || input == nil {
		return nil
	}
	if ref := fsxAdminPasswordRef(r.ko); ref != nil {
		if input.OntapConfiguration == nil || input.OntapConfiguration.FsxAdminPassword == nil {
			return errEmptySecret("Spec.OntapConfiguration.FsxAdminPassword", ref)
		}
	}
	if ref := selfManagedADPasswordRef(r.ko); ref != nil {
		smad := input.WindowsConfiguration
		if smad == nil || smad.SelfManagedActiveDirectoryConfiguration == nil ||
			smad.SelfManagedActiveDirectoryConfiguration.Password == nil {
			return errEmptySecret(
				"Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password", ref,
			)
		}
	}
	return nil
}

// terminalIfFailed returns a terminal error when FSx reports the file system as
// FAILED, which the API documents as unrecoverable.
//
// Without this, ResourceSynced stays False (synced.when lists only AVAILABLE)
// and the resource is requeued forever, calling DescribeFileSystems for a state
// that cannot change. The returned resource is non-nil so the runtime still
// patches Status, surfacing FailureDetails alongside the Terminal condition.
//
// Never while the CR is being deleted: the delete path calls ReadOne first and
// aborts on any error but NotFound, so returning terminal there would strand the
// finalizer -- and deletion is the recovery AWS documents for a FAILED file
// system.
func terminalIfFailed(ko *svcapitypes.FileSystem) error {
	if ko == nil || ko.Status.Lifecycle == nil {
		return nil
	}
	if ko.DeletionTimestamp != nil {
		return nil
	}
	if *ko.Status.Lifecycle != string(svcsdktypes.FileSystemLifecycleFailed) {
		return nil
	}
	reason := "no failure details reported"
	if ko.Status.FailureDetails != nil && ko.Status.FailureDetails.Message != nil {
		reason = *ko.Status.FailureDetails.Message
	}
	return ackerr.NewTerminalError(fmt.Errorf(
		"file system is in FAILED state and cannot recover: %s", reason,
	))
}

// setFileSystemStatus copies the FSx-owned state from an SDK FileSystem shape
// onto the supplied CR.
func setFileSystemStatus(
	ko *svcapitypes.FileSystem,
	fs *svcsdktypes.FileSystem,
) {
	if fs == nil {
		return
	}
	if fs.CreationTime != nil {
		ko.Status.CreationTime = &metav1.Time{Time: *fs.CreationTime}
	}
	if fs.DNSName != nil {
		ko.Status.DNSName = fs.DNSName
	}
	if fs.FailureDetails != nil {
		ko.Status.FailureDetails = &svcapitypes.FileSystemFailureDetails{
			Message: fs.FailureDetails.Message,
		}
	} else {
		ko.Status.FailureDetails = nil
	}
	if fs.FileSystemId != nil {
		ko.Status.FileSystemID = fs.FileSystemId
	}
	if fs.Lifecycle != "" {
		ko.Status.Lifecycle = aws.String(string(fs.Lifecycle))
	}
	if fs.NetworkInterfaceIds != nil {
		ko.Status.NetworkInterfaceIDs = aws.StringSlice(fs.NetworkInterfaceIds)
	}
	if fs.OwnerId != nil {
		ko.Status.OwnerID = fs.OwnerId
	}
	// ResourceARN is marked `is_arn: true` in generator.yaml, so the CRD has no
	// standalone Status.ResourceARN field: the value belongs in
	// Status.ACKResourceMetadata.ARN, which is what syncTags (and every ACK
	// consumer of the resource's ARN) reads.
	if fs.ResourceARN != nil {
		if ko.Status.ACKResourceMetadata == nil {
			ko.Status.ACKResourceMetadata = &ackv1alpha1.ResourceMetadata{}
		}
		arn := ackv1alpha1.AWSResourceName(*fs.ResourceARN)
		ko.Status.ACKResourceMetadata.ARN = &arn
	}
	if fs.VpcId != nil {
		ko.Status.VPCID = fs.VpcId
	}
}

// syncTags brings the file system's AWS tags in line with Spec.Tags using the
// FSx TagResource/UntagResource operations. FSx has no bulk "replace tags"
// call, so removals and additions are issued separately.
func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTags")
	defer func() { exit(err) }()

	// ResourceARN carries `is_arn: true` in generator.yaml, so sdkFind/sdkCreate
	// populate Status.ACKResourceMetadata.ARN and `latest` (which always comes
	// from a read) has it. The guard is purely defensive.
	arn := (*string)(nil)
	if latest.ko.Status.ACKResourceMetadata != nil && latest.ko.Status.ACKResourceMetadata.ARN != nil {
		arn = aws.String(string(*latest.ko.Status.ACKResourceMetadata.ARN))
	}
	if arn == nil {
		return fmt.Errorf("cannot sync tags: resource ARN is not yet known")
	}

	desiredTags, _ := convertToOrderedACKTags(desired.ko.Spec.Tags)
	latestTags, _ := convertToOrderedACKTags(latest.ko.Spec.Tags)

	var toRemove []string
	for k := range latestTags {
		if _, found := desiredTags[k]; !found {
			toRemove = append(toRemove, k)
		}
	}

	var toAdd []svcsdktypes.Tag
	for k, v := range desiredTags {
		if latestValue, found := latestTags[k]; !found || latestValue != v {
			toAdd = append(toAdd, svcsdktypes.Tag{
				Key:   aws.String(k),
				Value: aws.String(v),
			})
		}
	}

	if len(toRemove) > 0 {
		_, err = rm.sdkapi.UntagResource(ctx, &svcsdk.UntagResourceInput{
			ResourceARN: arn,
			TagKeys:     toRemove,
		})
		rm.metrics.RecordAPICall("UPDATE", "UntagResource", err)
		if err != nil {
			return err
		}
	}
	if len(toAdd) > 0 {
		_, err = rm.sdkapi.TagResource(ctx, &svcsdk.TagResourceInput{
			ResourceARN: arn,
			Tags:        toAdd,
		})
		rm.metrics.RecordAPICall("UPDATE", "TagResource", err)
		if err != nil {
			return err
		}
	}
	return nil
}

// customUpdateFileSystem replaces the generated sdkUpdate, which does not
// compile: FSx models each config block with three shapes (Create*, Update*,
// <Type>FileSystemConfiguration), and where their members diverge -- notably
// Add/RemoveRouteTableIds vs the Spec's single RouteTableIDs list -- code-gen
// emits references to Spec fields that do not exist.
func (rm *resourceManager) customUpdateFileSystem(
	ctx context.Context,
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) (updated *resource, err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.customUpdateFileSystem")
	defer func() { exit(err) }()

	// Start from the desired state and carry over everything FSx owns from the
	// most recent read, so a partial update never blanks out Status.
	ko := desired.ko.DeepCopy()
	ko.Status = *latest.ko.Status.DeepCopy()

	// Reject immutable value and presence changes before anything else: FSx
	// cannot apply them, and without this the reconciler would return the
	// desired object unchanged and repeat the same no-op every resync.
	if err = rejectImmutableFieldChanges(delta, desired, latest); err != nil {
		return nil, err
	}

	// Preflight BEFORE any side effect. A delta can mix a supported change with
	// an unsupported one (or with tags); syncing tags or sending the supported
	// half first would partially apply -- an irreversible storage increase or a
	// password rotation -- and only surface the rest on the next reconcile.
	if err = rm.rejectUnexpressibleChanges(ctx, desired, latest, delta); err != nil {
		return nil, err
	}

	// Tags are managed by TagResource/UntagResource rather than
	// UpdateFileSystem.
	if delta.DifferentAt("Spec.Tags") {
		if err = rm.syncTags(ctx, desired, latest); err != nil {
			return nil, err
		}
	}

	input, err := rm.newUpdateRequestPayload(ctx, desired, latest, delta)
	if err != nil {
		return nil, err
	}
	if input == nil {
		// Preflight guarantees every non-tag path is expressible, so reaching
		// here means tags were the only difference.
		rlog.Debug("only tags changed; skipping UpdateFileSystem")
		rm.setStatusDefaults(ko)
		return &resource{ko}, nil
	}

	var resp *svcsdk.UpdateFileSystemOutput
	resp, err = rm.sdkapi.UpdateFileSystem(ctx, input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateFileSystem", err)
	if err != nil {
		return nil, err
	}

	setFileSystemStatus(ko, resp.FileSystem)
	// Only advance the baseline for a password this request actually carried.
	// Advancing both after any successful update would mark an untouched
	// password as applied, hiding a later rotation of it.
	advanceLastAppliedSecretRefs(ko, requestCarriedPasswords(input))
	rm.setStatusDefaults(ko)
	return &resource{ko}, nil
}
