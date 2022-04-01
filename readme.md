## :zap:Quickstart

To run this project locally as well as running it in a kubernetes cluster you need to define your airflow variables in a file named variables.env and the airflow secrets in a secrets.env. You can follow the examples provided with the expected variables.

### Locally 
You can run this project locally just need to:
1. Have the variables MY_AIRFLOW_SERVICE_ACCOUNT and GOOGLE_APPLICATION_CREDENTIALS defined you can source them from your variables.env file. 
2. Go to the __local__ directory and start docker-compose 

``` shell
docker-compose up 
```

Go to http://localhost:8080 and access airflow with the airflow user and password.

### Kubernetes cluster

To run in a kubernetes cluster you can setup the infraestructure with the instructions in the gcp directory.
