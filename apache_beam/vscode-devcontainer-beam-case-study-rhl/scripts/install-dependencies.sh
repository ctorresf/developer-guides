#!/usr/bin/bash 
cp requirements.txt /tmp/
cd /tmp/
# install dependencies for dvirtz.parquet-viewer vscode plugin
sudo apt update
sudo apt install -y -V yarnpkg
# create a symbolic link to use yarn command
sudo ln -s /usr/bin/yarnpkg /usr/bin/yarn
yarn add parquet-wasm
# install python dependencies
python3 -m pip install --upgrade pip
pip3 install --user -r requirements.txt
#  Command line (CLI) tool to inspect Apache Parquet files on the go
pip install parquet-cli 
sudo apt install software-properties-common -y
sudo apt install  -y graphviz
pip install Graphviz