# Flink Client Project

## Overview
This project demonstrates how to run a Flink job locally. Follow the steps below to set up and run the application.

## Prerequisites
- Java JDK 8 or higher
- Maven 3.3 or higher
- Git

## Steps to Run the Application Locally

### 1. Download Kafka
1. Download Kafka from the official website: [Kafka Downloads](https://kafka.apache.org/downloads).
2. Extract the downloaded archive to a preferred directory.

### 2. Download Flink
1. Download Flink from the official website: [Flink Downloads](https://flink.apache.org/downloads.html).
2. Extract the downloaded archive to a preferred directory.

### 3. Clone the Flink Client from GitHub
1. Open a terminal and navigate to your preferred directory.
2. Clone the Flink client repository:
   ```sh
   git clone https://github.com/jest2code/flink-client
   cd flink-client
   ```
### 4. Start Kafka
   Navigate to the Kafka directory.
   Start Zookeeper:
   ```sh
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```
   Open another terminal and start Kafka:
   ```sh
   bin/kafka-server-start.sh config/server.properties
   ```
### 5. Update Flink Config File
   Navigate to the Flink directory.
   Open conf/flink-conf.yaml in a text editor.
   Update the following configurations:
   ```yaml
   taskmanager.numberOfTaskSlots: 12
   parallelism.default: 12
   ```
### 6. Copy JAR Files from Flink Client to Flink Lib
   Navigate to the flink-client/lib directory.
   Copy all JAR files to the flink/lib directory:
   ```sh
   cp flink-client/lib/*.jar flink/lib/
   ```
### 7. Start Flink Cluster
   Navigate to the Flink directory.
   Start the Flink cluster:
   ```sh
   bin/start-cluster.sh
   ```
### 8. Build the Maven Project Flink-Client
   Navigate to the flink-client directory.
   Build the project using Maven:
   ```sh
   mvn clean package
   ```
### 9. Submit the Job in Flink
   Use the built JAR file to submit the job:
   ```sh
   bin/flink run target/flink-client-1.0-SNAPSHOT.jar
   ```
### 10. Run the Producer Class
   * Open the KafkaProducerClient class in your IDE.
   * Run the KafkaProducer class to start producing messages to the Kafka topic.

### Additional Information
   * Ensure that the Kafka topic used in the Flink job is created and running.
   * Monitor the Flink Dashboard for job execution details and logs.
   * For any issues, refer to the Flink and Kafka official documentation.
   * Update the producer script as needed.
