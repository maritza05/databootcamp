#!/usr/bin/env bash

# Export environement variables
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Installing dependencies here, installing directly in
# the Dockerfile doesn't work for gcsfs
pip install -r requirements.txt

# Initiliase the metadatabase
airflow db init

# Create User
airflow users create -e "admin@airflow.com" -f "airflow" -l "airflow" -p "airflow" -r "Admin" -u "airflow"

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web sever in foreground (for docker logs)
exec airflow webserver

exec /entrypoint "${@}"
