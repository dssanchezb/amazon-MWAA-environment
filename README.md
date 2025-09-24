<h1 align="center"> <img src="assets/imgs/airflow_spin.gif" alt="Alt text" width="30" height="30"> AMAZON MWAA ENVIRONMENT <img src="assets/imgs/airflow_spin.gif" alt="Alt text" width="30" height="30"></h1>

## Project Description

This repository contains a template for local development of Apache Airflow DAGs (Directed Acyclic Graphs). It provides a [Podman-based](https://podman.io) setup for running Airflow locally, making it easy to develop, test, and manage workflows efficiently.

The project includes:
- A Podman Compose configuration file that sets up all necessary services
- A `dags` folder for storing and developing Airflow DAGs
- Configuration files for setting up the Airflow environment

## Setup Instructions

### Prerequisites

- Podman and Podman Compose installed on your system
- Git (for cloning the repository)

> **Note:** You can also use [Docker](https://www.docker.com) to execute this project. <img src="assets/imgs/old-man-yelling-at-airflow.gif" alt="Alt text" width="30" height="30">

### Steps to Set Up

1. Clone the repository:
   ```
   git clone git@github.com:ejguerra_nequi/amazon-MWAA-environment.git
   cd amazon-MWAA-environment/
   ```

2. Install [pipenv](https://pipenv.pypa.io/en/latest/):

   ```
   pip3 install pipenv
   ```

3. Create the virtual environment with all the dependencies using `pipenv`:

   ```
   python3 -m pipenv install
   ```

4. Activate the virtual environment:

   ```
   python3 -m pipenv shell
   ```

5. Edit the `.env` file in the project root using the values that fit your laptop's structure

6. Start the Airflow services:
   ```
   podman-compose up -d
   ```

## Usage

Once the setup is complete and all services are running, you can access the Airflow web interface:

1. Open a web browser and navigate to `http://localhost:8080`
2. Log in using the default credentials:
   - Username: airflow
   - Password: airflow

From the Airflow web interface, you can:
- View and manage DAGs
- Monitor task execution
- Access logs
- Trigger DAG runs manually

### Managing DAGs

- Place your DAG files in the `dags/` directory of the project.
- Airflow will automatically pick up new or modified DAGs from this directory.

### Stopping the Environment

To stop the Airflow services:
```
podman-compose down
```

To stop the services and delete volumes (this will reset the database) <img src="assets/imgs/airflow-tire-fire.gif" alt="Alt text" width="30" height="30">:
```
podman-compose down --volumes --remove-orphans
```

## Additional Information

- The Airflow web server is accessible at `http://localhost:8080`
- PostgreSQL is used as the metadata database
- Redis is used as the message broker for the Celery executor
- The project uses Airflow version 2.8.1 (as specified in the podman-compose.yaml)

For more detailed information about Apache Airflow and its configuration, please refer to the [official Airflow documentation](https://airflow.apache.org/docs/).