#!/bin/bash

# Configure AWS CLI with your AWS Access Key ID, Secret Access Key, default region, and default output format
echo "Configuring AWS CLI..."
aws configure

# Configure Git credentials to work with CodeCommit
echo "Configuring Git credentials for CodeCommit..."
git config --global credential.helper '!aws codecommit credential-helper $@'
git config --global credential.UseHttpPath true

echo "AWS CodeCommit setup complete."
