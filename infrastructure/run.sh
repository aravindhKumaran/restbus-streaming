#!/bin/bash

# Function to check if the previous command was successful
function check_status() {
  if [ $? -ne 0 ]; then
    echo "Error: Script failed to execute."
    exit 1
  fi
}

# Run the mysql setup
echo "Running Script 1..."
sudo chmod 766 /home/ubuntu/infrastructure/mysql/setup_mysql.sh 
/home/ubuntu/infrastructure/mysql/setup_mysql.sh
check_status

# Run the nifi setup
echo "Running Script 2..."
sudo chmod 766 /home/ubuntu/infrastructure/nifi/setup_nifi.sh 
/home/ubuntu/infrastructure/nifi/setup_nifi.sh
check_status

# Run the kafka setup
echo "Running Script 3..."
sudo chmod 766 /home/ubuntu/infrastructure/kafka/setup_kafka.sh 
/home/ubuntu/infrastructure/kafka/setup_kafka.sh
check_status

# Run the msk-connect setup
echo "Running Script 4..."
sudo chmod 766 /home/ubuntu/infrastructure/msk-connect/setup_connect.sh 
/home/ubuntu/infrastructure/msk-connect/setup_connect.sh
check_status

echo "All scripts executed successfully!"