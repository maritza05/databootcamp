
FROM	apache/airflow:2.2.3-python3.7
USER	root
COPY	./requirements.txt	./requirements.txt
COPY	./start_airflow.sh	./start_airflow.sh
RUN	chmod	+x	./start_airflow.sh
USER	airflow
EXPOSE	8080
ENTRYPOINT	["./start_airflow.sh"]
