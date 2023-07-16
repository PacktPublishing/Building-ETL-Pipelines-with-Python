#!/bin/bash
# Docker with LocalStack are popular open-source tool for local AWS cloud Development and Testing
## The following bash script that sets up and installs Docker along with LocalStack:

# Update package lists
sudo apt-get update

# Install Docker dependencies
sudo apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common

# Add Docker's official GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Add Docker's stable repository
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update package lists again
sudo apt-get update

# Install Docker Engine
sudo apt-get install -y docker-ce docker-ce-cli containerd.io

# Add the current user to the "docker" group
sudo usermod -aG docker $USER

# Install LocalStack
sudo pip install localstack

# Start LocalStack
localstack start

# Print Docker and LocalStack versions
docker version
localstack --version
