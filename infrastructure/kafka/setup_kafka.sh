#!/bin/bash

cd /home/ubuntu/downloads

if [ ! -f kafka_2.12-2.8.1.tgz ]; then
    # Download Kafka
    echo "Downloading Kafka in src directory..."
    
    wget https://archive.apache.org/dist/kafka/3.3.2/kafka_2.12-3.3.2.tgz
    tar -xzf kafka_2.12-3.3.2.tgz
    rm kafka_2.12-3.3.2.tgz
    
    # Go to the kafka_2.12-2.8.1/bin directory
    cd kafka_2.12-3.3.2/bin

    # Create client.properties file and add the security.protocol 
    echo "security.protocol=PLAINTEXT" > client.properties
fi

echo "Kafka setup completed successfully!"