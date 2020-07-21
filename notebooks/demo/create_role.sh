#!/usr/bin/env bash
# Copyright (c) 2020 Grid Dynamics International, Inc. All Rights Reserved
# http://www.griddynamics.com
# Classification level: PUBLIC
# Licensed under the Apache License, Version 2.0(the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Id:          ML-DSL
# Project:     ML DSL
# Description: DSL to configure and execute ML/DS pipelines

ROLE_NAME=SageMakerRoleMlDsl
# WARNING: this policy gives full S3 access to container that is running in SageMaker.
# You can change this policy to a more restrictive one, or create your own policy.
POLICY=arn:aws:iam::aws:policy/AmazonS3FullAccess
# Creates a AWS policy that allows the role to interact with any S3 bucket
cat <<EOF > /tmp/sm-role-policy-doc.json
{
	"Version": "2012-10-17",
	"Statement": [{
		"Effect": "Allow",
		"Principal": {
			"Service": "sagemaker.amazonaws.com"
		},
		"Action": "sts:AssumeRole"
	}]
}
EOF
# Role
aws iam create-role --role-name ${ROLE_NAME} --assume-role-policy-document file:///tmp/sm-role-policy-doc.json
# S3 full access policy
aws iam attach-role-policy --policy-arn ${POLICY}  --role-name ${ROLE_NAME}