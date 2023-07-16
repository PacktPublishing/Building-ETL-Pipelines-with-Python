#!/bin/bash
# Docker with LocalStack are popular open-source tool for local AWS cloud Development and Testing
## Execute the following script to install AWS CLI and SAM CLI, Docker, and LocalStack:

# Update package lists
sudo apt-get update

# (1) Install AWS CLI
curl "https://d1vvhvl2y92vvt.cloudfront.net/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf awscliv2.zip aws

# (2) Install AWS SAM CLI
pip install --user aws-sam-cli

# (3) Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
sudo systemctl enable docker

# (3.5) Install Docker Engine
sudo apt-get install -y docker-ce docker-ce-cli containerd.io
sudo usermod -aG docker $USER

# (4) Install LocalStack
sudo pip install localstack
localstack start # Start LocalStack

# Print versions
aws --version
sam --version
docker --version
localstack --version
