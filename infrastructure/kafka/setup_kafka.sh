#!/bin/bash

cd /home/ubuntu/src

if [ ! -f kafka_2.12-2.8.1.tgz ]; then
    # Download Kafka
    echo "Downloading Kafka in src directory..."
    wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
    tar -xzf kafka_2.12-2.8.1.tgz
    rm kafka_2.12-2.8.1.tgz
    
    # Go to the kafka_2.12-2.8.1/bin directory
    cd kafka_2.12-2.8.1/bin

    # Create client.properties file and add the security.protocol 
    echo "security.protocol=PLAINTEXT" > client.properties
fi

echo "Kafka setup completed successfully!"