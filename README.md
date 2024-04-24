# Cloud-technology
Distributed Data Processing System for HZZ Analysis
This project contains a distributed system for processing particle physics data from the ATLAS experiment, specifically focusing on the HZZ analysis. The system is designed to be scalable and efficient, utilising Docker Swarm for container orchestration and RabbitMQ for message brokering.

## System Overview

The HZZ Analysis System is composed of three main services:

- **Preprocess Service**: Prepares and queues tasks for processing.
- **Process Service**: Handles the computational workload in parallel.
- **Plot Service**: Aggregates processed data and generates visualizations.

## Getting Started


### Installation

1. **Clone the repository**
    ```sh
    git clone git@github.com:ruyuG/Cloud-technology.git
    cd Cloud-technology
    ```

2. **Set up the Docker Swarm**
    Make sure Docker Swarm is initialized and ready to use.

3. **Launch RabbitMQ**
    Start the RabbitMQ service using the official image:
    ```sh
    docker service create --name rabbitmq --publish published=5672,target=5672 --publish published=15672,target=15672 rabbitmq:3-management
    ```

4. **Build and Push Images**
    Build the Docker images for each service and push them to your local Docker registry.
    ```sh
    docker build -t 192.168.56.101:5000/preprocess-service:latest ./preprocess
    docker push 192.168.56.101:5000/preprocess-service:latest
    # Repeat for process-service and plot-service
    ```

5. **Deploy with Docker Compose**
    Update docker-compose.yml with the correct image tags and then deploy:
    ```sh
    docker stack deploy -c docker-compose.yml HZZ
    ```

## Usage

After deployment, the system will begin processing data based on the tasks queued in RabbitMQ.


