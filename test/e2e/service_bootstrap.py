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
"""Bootstraps the resources required to run the FSx integration tests.
"""
import logging

from acktest.bootstrapping import Resources, BootstrapFailureException
from acktest.bootstrapping.vpc import VPC

from e2e import bootstrap_directory
from e2e.bootstrap_resources import BootstrapResources

def service_bootstrap() -> Resources:
    logging.getLogger().setLevel(logging.INFO)

    resources = BootstrapResources(
        # One private subnet is enough for the single-AZ deployment types the
        # tests use; FSx needs no internet egress to be created.
        FSxVPC=VPC(
            name_prefix="fsx-vpc",
            num_public_subnet=0,
            num_private_subnet=1,
            private_subnet_cidr_blocks=["10.0.0.0/20"],
            # CreateFileSystem rejects a Lustre file system whose security
            # groups do not permit LNET traffic on port 988
            # (InvalidNetworkSettings, a terminal code). A self-referencing
            # all-protocols rule is what the FSx docs recommend.
            security_group_self_referencing_ingress=True,
        ),
    )

    try:
        resources.bootstrap()
    except BootstrapFailureException as ex:
        exit(254)

    return resources

if __name__ == "__main__":
    config = service_bootstrap()
    # Write config to current directory by default
    config.serialize(bootstrap_directory)
