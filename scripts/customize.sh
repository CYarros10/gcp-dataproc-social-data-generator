#! /usr/bin/bash

apt-get update -y
apt-get install wget -y
apt-get install zstd -y
apt-get install python3-pip -y

pip3 install --upgrade textblob zstandard better_profanity textstat google-cloud-storage

python3 -m textblob.download_corpora