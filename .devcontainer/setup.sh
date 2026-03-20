#!/bin/bash

# System deps for building Python C extensions (e.g. lz4)
sudo apt-get update && sudo apt-get install -y python3-dev build-essential

# Java (for Spark)
sudo apt-get install -y openjdk-17-jdk

# Symlink Python 3.13 headers for Bruin's embedded Python 3.11
sudo ln -sf /usr/local/include/python3.13 /usr/include/python3.11

# Python packages
pip install pandas sqlalchemy psycopg2-binary pyarrow click google-cloud-storage pyspark uv
pip install -r requirements.txt || echo 'No requirements.txt found'

# Bruin CLI
curl -LsSf https://getbruin.com/install/cli | sh

# Shell config
echo 'export GOOGLE_APPLICATION_CREDENTIALS=/workspaces/datatalks-zoomcamp/pipeline/service-account.json' >> ~/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64' >> ~/.bashrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc
echo 'PS1="> "' >> ~/.bashrc