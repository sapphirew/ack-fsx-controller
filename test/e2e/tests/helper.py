# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Helper functions for FSx e2e tests.
"""

import logging
import time
from typing import Dict, Optional

from acktest.k8s import condition
from acktest.k8s import resource as k8s
from botocore.exceptions import ClientError


# The only errors get_file_system translates to None.
NOT_FOUND_ERROR_CODES = frozenset(
    ("FileSystemNotFound", "ResourceNotFoundException")
)


def wait_for_synced_or_terminal(
    ref, wait_periods: int, period_length: int,
) -> None:
    """Waits for ACK.ResourceSynced=True, failing fast on ACK.Terminal.

    acktest's wait_on_condition polls the full timeout regardless, so a
    terminal create error costs the whole wait and surfaces as a bare
    `assert False` with the reason only in the controller logs.
    """
    for _ in range(wait_periods):
        cr = k8s.get_resource(ref)
        conditions = (cr or {}).get("status", {}).get("conditions") or []
        by_type = {c["type"]: c for c in conditions}

        terminal = by_type.get(condition.CONDITION_TYPE_TERMINAL)
        if terminal is not None and terminal.get("status") == "True":
            raise AssertionError(
                f"{ref.name} went terminal instead of syncing: "
                f"{terminal.get('message')}"
            )

        synced = by_type.get(condition.CONDITION_TYPE_RESOURCE_SYNCED)
        if synced is not None and synced.get("status") == "True":
            return

        time.sleep(period_length)

    # Report the last-known conditions so a real timeout is diagnosable.
    cr = k8s.get_resource(ref)
    conditions = (cr or {}).get("status", {}).get("conditions") or []
    raise AssertionError(
        f"{ref.name} did not reach ACK.ResourceSynced=True within "
        f"{wait_periods * period_length}s; conditions: "
        + "; ".join(
            f"{c.get('type')}={c.get('status')} ({c.get('message')})"
            for c in conditions
        )
    )


class FSxValidator:
    def __init__(self, fsx_client):
        self.fsx_client = fsx_client

    def get_file_system(self, file_system_id: str) -> Optional[dict]:
        """Returns the FSx FileSystem with the supplied ID, or None if it does
        not exist (or has already been fully deleted).

        Only a genuine not-found error is translated to None. Any other failure
        is re-raised: wait_until_gone() treats None as "deleted in AWS", so
        swallowing a transient error (throttling, expired credentials, a
        connection failure) would let the delete assertions pass while a
        multi-hundred-GiB file system is still running and billing.
        """
        try:
            resp = self.fsx_client.describe_file_systems(
                FileSystemIds=[file_system_id],
            )
            file_systems = resp.get("FileSystems", [])
            if len(file_systems) == 0:
                return None
            return file_systems[0]
        except self.fsx_client.exceptions.FileSystemNotFound:
            return None
        except ClientError as e:
            # Some botocore/endpoint combinations surface the modeled 404 as a
            # generic ClientError rather than the FileSystemNotFound subclass.
            code = e.response.get("Error", {}).get("Code")
            if code in NOT_FOUND_ERROR_CODES:
                return None
            logging.error(
                f"describe_file_systems({file_system_id}) failed with {code}"
            )
            raise

    def file_system_exists(self, file_system_id: str) -> bool:
        return self.get_file_system(file_system_id) is not None

    def get_lifecycle(self, file_system_id: str) -> Optional[str]:
        fs = self.get_file_system(file_system_id)
        if fs is None:
            return None
        return fs.get("Lifecycle")

    def get_tags(self, file_system_id: str) -> Dict[str, str]:
        fs = self.get_file_system(file_system_id)
        if fs is None:
            return {}
        return {t["Key"]: t["Value"] for t in fs.get("Tags", [])}

    def wait_until_lustre_maintenance_window(
        self,
        file_system_id: str,
        expected: str,
        wait_periods: int = 20,
        period_length: int = 15,
    ) -> bool:
        """Polls DescribeFileSystems until the Lustre weekly maintenance window
        matches `expected`.

        FSx leaves Lifecycle at AVAILABLE while a maintenance-window change is
        applied, so the controller's ResourceSynced condition can already be
        True from the *pre-update* reconcile when the test looks at it. Polling
        the AWS API directly is the only reliable signal that the update landed.
        """
        for _ in range(wait_periods):
            fs = self.get_file_system(file_system_id)
            if fs is not None:
                actual = fs.get("LustreConfiguration", {}).get(
                    "WeeklyMaintenanceStartTime"
                )
                if actual == expected:
                    return True
                logging.info(
                    f"file system {file_system_id} maintenance window is "
                    f"{actual}; waiting for {expected}"
                )
            time.sleep(period_length)
        return False

    def wait_until_tag(
        self,
        file_system_id: str,
        key: str,
        value: str,
        wait_periods: int = 20,
        period_length: int = 15,
    ) -> bool:
        """Polls DescribeFileSystems until the supplied tag has the supplied
        value.

        As with the maintenance window, a tag change does not move the file
        system out of AVAILABLE, so the Synced condition is not a usable signal.
        """
        for _ in range(wait_periods):
            tags = self.get_tags(file_system_id)
            if tags.get(key) == value:
                return True
            logging.info(
                f"file system {file_system_id} tag {key} is {tags.get(key)}; "
                f"waiting for {value}"
            )
            time.sleep(period_length)
        return False

    def wait_until_gone(
        self,
        file_system_id: str,
        wait_periods: int = 40,
        period_length: int = 30,
    ) -> bool:
        """Polls DescribeFileSystems until the file system disappears.

        FSx keeps a file system in the DELETING lifecycle state for several
        minutes after DeleteFileSystem returns, so "deleted from Kubernetes"
        is not the same as "deleted in AWS".
        """
        for _ in range(wait_periods):
            fs = self.get_file_system(file_system_id)
            if fs is None:
                return True
            logging.info(
                f"file system {file_system_id} still present in state "
                f"{fs.get('Lifecycle')}; waiting"
            )
            time.sleep(period_length)
        return self.get_file_system(file_system_id) is None
