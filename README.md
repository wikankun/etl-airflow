# ETL Airflow

ETL Learning Project Using Airflow

## Prerequisite

- Docker
- Docker Compose

## How to run

- On Linux, the mounted volumes in container use the native Linux filesystem user/group permissions, so you have to make sure the container and host computer have matching file permissions.

    `mkdir ./dags ./data ./logs ./output ./plugins`

    `echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env`

- Run database migrations and create the first user account.

    `docker-compose up airflow-init`

    The account created has the login `airflow` and the password `airflow`.

- Before running docker compose, you need to build image because there's some python packages we need.

    `docker build .`

- Now run all services

    `docker-compose up`

- After it's all set, access the webserver

    `http://localhost:8080/`
