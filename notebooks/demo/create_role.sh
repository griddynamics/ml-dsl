#!/usr/bin/env bash
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