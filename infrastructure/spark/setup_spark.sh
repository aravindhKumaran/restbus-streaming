#!/bin/bash

cd /home/ubuntu/downloads

if [ ! -f spark-3.3.2-bin-hadoop3.tgz ]; then
    # Download spark
    echo "Downloading spark in downloads directory..."
    
    wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
    tar -xzf spark-3.3.2-bin-hadoop3.tgz
    rm spark-3.3.2-bin-hadoop3.tgz
    
fi

echo "Spark setup completed successfully!"