#!/bin/bash

# Create the downloads directory if it doesn't exist
mkdir -p /home/ubuntu/downloads

# Change to the downloads directory
cd /home/ubuntu/downloads

if [ ! -f spark-3.3.2-bin-hadoop3.tgz ]; then
    # Download spark
    echo "Downloading spark in downloads directory..."
    
    wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
    tar -xzf spark-3.3.2-bin-hadoop3.tgz
    rm spark-3.3.2-bin-hadoop3.tgz
    
fi

# Get the absolute path to the Spark directory
spark_dir=$(realpath spark-3.3.2-bin-hadoop3)

# Update the ~/.bashrc file to export Spark home and add Spark to the PATH
echo "export SPARK_HOME=$spark_dir" >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc

# Load the updated ~/.bashrc
source ~/.bashrc

echo "Spark setup and environment variables updated successfully!"