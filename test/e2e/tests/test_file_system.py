# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
# 	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the FSx FileSystem resource.

Uses Lustre PERSISTENT_2: cheapest and fastest to stand up, and needs neither
an Active Directory (WINDOWS) nor an admin password Secret (ONTAP).
"""

import logging
import time

import pytest

from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name

from e2e import CRD_GROUP, CRD_VERSION, load_fsx_resource, service_marker
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e.tests.helper import FSxValidator, wait_for_synced_or_terminal

RESOURCE_PLURAL = "filesystems"

# PERSISTENT_2 requires a storage capacity of at least 1200 GiB.
STORAGE_CAPACITY = 1200

INITIAL_MAINTENANCE_WINDOW = "2:03:00"
UPDATED_MAINTENANCE_WINDOW = "4:05:30"

INITIAL_TAG_KEY = "ack-fsx-e2e"
INITIAL_TAG_VALUE = "initial"
UPDATED_TAG_VALUE = "updated"

# An FSx for Lustre file system typically reaches AVAILABLE in 5-10 minutes and
# disappears from DescribeFileSystems 5-10 minutes after being deleted.
CREATE_WAIT_AFTER_SECONDS = 30
CREATE_CONDITION_WAIT_PERIODS = 40
CREATE_CONDITION_PERIOD_LENGTH = 30

MODIFY_WAIT_AFTER_SECONDS = 30
MODIFY_CONDITION_WAIT_PERIODS = 30
MODIFY_CONDITION_PERIOD_LENGTH = 30

DELETE_WAIT_AFTER_SECONDS = 30
# sdkDelete requeues until DescribeFileSystems stops reporting the file system,
# so the finalizer is only dropped once FSx has fully deleted it -- allow the
# full 5-10 minute DELETING window plus margin.
DELETE_CONDITION_WAIT_PERIODS = 30


@pytest.fixture(scope="module")
def lustre_file_system(fsx_client):
    """Creates a Lustre file system, waits for sync, tears it down after the
    module's tests. Delete assertions live here so teardown always runs.
    """
    resource_name = random_suffix_name("ack-fsx-lustre", 32)

    replacements = REPLACEMENT_VALUES.copy()
    replacements["FILE_SYSTEM_NAME"] = resource_name
    replacements["STORAGE_CAPACITY"] = str(STORAGE_CAPACITY)
    replacements["WEEKLY_MAINTENANCE_START_TIME"] = INITIAL_MAINTENANCE_WINDOW
    replacements["TAG_KEY"] = INITIAL_TAG_KEY
    replacements["TAG_VALUE"] = INITIAL_TAG_VALUE

    resource_data = load_fsx_resource(
        "file_system",
        additional_replacements=replacements,
    )
    logging.debug(resource_data)

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
        resource_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)

    # Wrapped so a mid-setup failure still tears down: a leaked 1200 GiB
    # PERSISTENT_2 file system is expensive.
    try:
        time.sleep(CREATE_WAIT_AFTER_SECONDS)
        cr = k8s.wait_resource_consumed_by_controller(ref)
        assert cr is not None
        assert k8s.get_resource_exists(ref)

        # The controller only reports Synced once the file system reaches the
        # AVAILABLE lifecycle state (see `synced.when` in generator.yaml).
        # A terminal CreateFileSystem error aborts the wait immediately rather
        # than burning the full 20 minutes -- the runtime never retries those.
        wait_for_synced_or_terminal(
            ref,
            wait_periods=CREATE_CONDITION_WAIT_PERIODS,
            period_length=CREATE_CONDITION_PERIOD_LENGTH,
        )

        cr = k8s.get_resource(ref)
        assert "status" in cr
        assert "fileSystemID" in cr["status"]
        file_system_id = cr["status"]["fileSystemID"]

        yield (ref, cr, file_system_id)
    finally:
        # ---- delete ----
        # ID may be unknown if setup failed before create reached AWS.
        file_system_id = None
        if k8s.get_resource_exists(ref):
            cr = k8s.get_resource(ref)
            if cr is not None and "status" in cr:
                file_system_id = cr["status"].get("fileSystemID")

            _, deleted = k8s.delete_custom_resource(
                ref,
                wait_periods=DELETE_CONDITION_WAIT_PERIODS,
                period_length=DELETE_WAIT_AFTER_SECONDS,
            )
            assert deleted
            assert not k8s.get_resource_exists(ref)

        # Confirm it is gone from AWS too, not just Kubernetes.
        if file_system_id is not None:
            validator = FSxValidator(fsx_client)
            assert validator.wait_until_gone(file_system_id)


@service_marker
@pytest.mark.canary
class TestFileSystem:
    def test_create_sets_status_and_creates_in_aws(
        self, fsx_client, lustre_file_system,
    ):
        (ref, _, file_system_id) = lustre_file_system
        assert file_system_id is not None

        # Verify via the CR.
        cr = k8s.get_resource(ref)
        condition.assert_synced(ref)
        assert cr["status"]["lifecycle"] == "AVAILABLE"
        assert cr["status"]["dnsName"] is not None
        assert cr["status"]["vpcID"] is not None
        assert cr["status"]["ackResourceMetadata"]["arn"] is not None
        assert cr["spec"]["fileSystemType"] == "LUSTRE"
        assert cr["spec"]["storageCapacity"] == STORAGE_CAPACITY

        # Verify via the AWS API.
        validator = FSxValidator(fsx_client)
        fs = validator.get_file_system(file_system_id)
        assert fs is not None
        assert fs["Lifecycle"] == "AVAILABLE"
        assert fs["FileSystemType"] == "LUSTRE"
        assert fs["StorageCapacity"] == STORAGE_CAPACITY
        assert fs["LustreConfiguration"]["DeploymentType"] == "PERSISTENT_2"
        assert (
            fs["LustreConfiguration"]["WeeklyMaintenanceStartTime"]
            == INITIAL_MAINTENANCE_WINDOW
        )
        assert validator.get_tags(file_system_id)[INITIAL_TAG_KEY] == INITIAL_TAG_VALUE

    def test_update_maintenance_window(self, fsx_client, lustre_file_system):
        """Exercises customUpdateFileSystem: a nested Spec.LustreConfiguration
        change must become an UpdateFileSystemLustreConfiguration.
        """
        (ref, _, file_system_id) = lustre_file_system

        updates = {
            "spec": {
                "lustreConfiguration": {
                    "weeklyMaintenanceStartTime": UPDATED_MAINTENANCE_WINDOW,
                },
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        # The file system stays AVAILABLE through the change, so Synced may
        # still be True from the pre-update reconcile. Poll AWS instead.
        validator = FSxValidator(fsx_client)
        assert validator.wait_until_lustre_maintenance_window(
            file_system_id, UPDATED_MAINTENANCE_WINDOW,
        )

        assert k8s.wait_on_condition(
            ref,
            condition.CONDITION_TYPE_RESOURCE_SYNCED,
            "True",
            wait_periods=MODIFY_CONDITION_WAIT_PERIODS,
            period_length=MODIFY_CONDITION_PERIOD_LENGTH,
        )

        # Verify via the CR.
        cr = k8s.get_resource(ref)
        assert (
            cr["spec"]["lustreConfiguration"]["weeklyMaintenanceStartTime"]
            == UPDATED_MAINTENANCE_WINDOW
        )

    def test_update_tags(self, fsx_client, lustre_file_system):
        """Exercises syncTags, which uses TagResource/UntagResource rather than
        UpdateFileSystem.
        """
        (ref, _, file_system_id) = lustre_file_system

        updates = {
            "spec": {
                "tags": [
                    {"key": INITIAL_TAG_KEY, "value": UPDATED_TAG_VALUE},
                ],
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        # Also covers Status.ACKResourceMetadata.ARN being populated -- without
        # it syncTags cannot call TagResource at all.
        validator = FSxValidator(fsx_client)
        assert validator.wait_until_tag(
            file_system_id, INITIAL_TAG_KEY, UPDATED_TAG_VALUE,
        )

        assert k8s.wait_on_condition(
            ref,
            condition.CONDITION_TYPE_RESOURCE_SYNCED,
            "True",
            wait_periods=MODIFY_CONDITION_WAIT_PERIODS,
            period_length=MODIFY_CONDITION_PERIOD_LENGTH,
        )

        # Verify via the CR.
        cr = k8s.get_resource(ref)
        cr_tags = {t["key"]: t["value"] for t in cr["spec"]["tags"]}
        assert cr_tags[INITIAL_TAG_KEY] == UPDATED_TAG_VALUE
