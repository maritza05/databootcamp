#!/usr/bin/env bash

# Export environement variables
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Initiliase the metadatabase
airflow db init

# FIXME: Installing gcsfs here because when installing in Dockerfile
# there's a dependency issue so this is a workaround
pip install gcsfs==2022.2.0


# Create User
airflow users create -e "admin@airflow.com" -f "airflow" -l "airflow" -p "airflow" -r "Admin" -u "airflow"

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web sever in foreground (for docker logs)
exec airflow webserver

exec /entrypoint "${@}"
