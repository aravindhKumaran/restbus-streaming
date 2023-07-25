#!/bin/bash

sudo apt update -y
sudo apt upgrade -y
sudo apt install docker.io -y
sudo chmod 666 /var/run/docker.sock
sudo apt install openjdk-11-jre-headless -y 
sudo apt install unzip -y
sudo apt install tar -y
sudo apt  install awscli -y
sudo apt install python3-pip -y