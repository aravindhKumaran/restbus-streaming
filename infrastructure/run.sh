#!/bin/bash

# Function to check if the previous command was successful
function check_status() {
  if [ $? -ne 0 ]; then
    echo "Error: Script failed to execute."
    exit 1
  fi
}

export base_path=/home/ubuntu/aws_restbus_proj/infrastructure

# Run the mysql setup
echo "Running mysql setup..."
sudo chmod 766 $base_path/mysql/setup_mysql.sh 
$base_path/mysql/setup_mysql.sh
check_status

# Run the nifi setup
echo "Running nifi setup..."
sudo chmod 766 $base_path/nifi/setup_nifi.sh 
$base_path/nifi/setup_nifi.sh
check_status

#Run the kafka setup
echo "Running kafka setup..."
sudo chmod 766 $base_path/kafka/setup_kafka.sh 
$base_path/kafka/setup_kafka.sh
check_status

# Run the msk-connect setup
echo "Running msk-connect setup..."
sudo chmod 766 $base_path/msk-connect/setup_connect.sh 
$base_path/msk-connect/setup_connect.sh
check_status

# Run the Spark setup
echo "Running Spark setup..."
sudo chmod 766 $base_path/spark/setup_spark.sh 
$base_path/spark/setup_spark.sh
check_status

# Run the superset setup
echo "Running superset setup..."
sudo chmod 766 $base_path/superset/setup_superset.sh
$base_path/superset/setup_superset.sh
check_status

echo "All scripts executed successfully!"