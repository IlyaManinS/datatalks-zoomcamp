#!/bin/bash

# System deps for building Python C extensions (e.g. lz4)
sudo apt-get update && sudo apt-get install -y python3-dev build-essential

# Symlink Python 3.13 headers for Bruin's embedded Python 3.11
sudo ln -sf /usr/local/include/python3.13 /usr/include/python3.11

# Python packages
pip install pandas sqlalchemy psycopg2-binary pyarrow click google-cloud-storage
pip install -r requirements.txt || echo 'No requirements.txt found'

# Bruin CLI
curl -LsSf https://getbruin.com/install/cli | sh

# Shell config
echo 'export GOOGLE_APPLICATION_CREDENTIALS=/workspaces/docker-workshop/pipeline/service-account.json' >> ~/.bashrc
echo 'PS1="> "' >> ~/.bashrc