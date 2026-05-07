#!/usr/bin/bash 
set -euo pipefail

# Configure passwordless sudo for rabbitmq and gcloud commands early
#if ! sudo grep -q "rabbitmq\|gcloud" /etc/sudoers.d/vscode 2>/dev/null; then
#  echo "Configuring passwordless sudo access..."
#  sudo bash -c 'echo "vscode ALL=(ALL) NOPASSWD: /usr/sbin/rabbitmq-server, /usr/sbin/rabbitmqctl, /usr/sbin/rabbitmq-plugins, /usr/bin/gcloud" >> /etc/sudoers.d/vscode'
#fi

cp requirements.txt /tmp/
cd /tmp/
# install dependencies for dvirtz.parquet-viewer vscode plugin
sudo apt update
sudo apt install -y -V yarnpkg curl apt-transport-https ca-certificates gnupg rabbitmq-server
# create a symbolic link to use yarn command
sudo ln -sf /usr/bin/yarnpkg /usr/bin/yarn
yarn add parquet-wasm
# enable RabbitMQ management plugin
sudo rabbitmq-plugins enable rabbitmq_management

# install the Google Cloud SDK
sudo apt-get install ca-certificates gnupg curl
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt update
sudo apt-get install -y google-cloud-cli

# install python dependencies
python3 -m pip install --upgrade pip
pip3 install --user -r requirements.txt
# Command line (CLI) tool to inspect Apache Parquet files on the go
pip3 install parquet-cli 
sudo apt install software-properties-common -y
sudo apt install -y graphviz
pip3 install Graphviz