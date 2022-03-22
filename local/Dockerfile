FROM apache/airflow:2.2.3

USER root

RUN pip install apache-airflow-providers-slack==4.0.1 && \
    pip install apache-airflow-providers-http==2.0.1 && \
    pip install apache-airflow-providers-google==6.2.0 && \
    pip install apache-airflow-providers-postgres==2.4.0 && \
    pip install pandas==1.3.5 && \
    pip install fsspec==2021.11.1 && \
    pip install gcsfs

# Copy the entrypoint.sh from host to container (at path AIRFLOW_HOME)
COPY ./start-airflow.sh ./start-airflow.sh

# Set the entrypoint.sh file to be executable
RUN chmod +x ./start-airflow.sh

# Set the username to use
USER airflow

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

# Execute start-airflow.sh
ENTRYPOINT [ "./start-airflow.sh" ]
