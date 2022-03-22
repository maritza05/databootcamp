# airflow

### Prerequisites
- Helm 3

#### Dependencies
- Kubernetes cluster version: 1.20 

### Storage
Create a namespace for storage deployment:
```
kubectl create namespace storage
```
Add the chart for the nfs-provisioner
```
helm repo add nfs-subdir-external-provisioner https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/
```
Install nfs-external-provisioner
```
helm install nfs-subdir-external-provisioner nfs-subdir-external-provisioner/nfs-subdir-external-provisioner \
    --namespace storage \
    --set nfs.server=$NFS_SERVER \
    --set nfs.path=/
```
## For Airflow

Here we are using official Airflow helm chart as example, but, can also been installed any other Airflow distribution.

Create the namespace
```
kubectl create namespace airflow
```

Add the chart repository and confirm:
```
helm repo add apache-airflow https://airflow.apache.org
```

Update the file `airflow-values.yaml` attributes; repo, branch and subPath of your DAGs. 
```yaml
    gitSync:
    enabled: true

    # git repo clone url
    # ssh examples ssh://git@github.com/apache/airflow.git
    # git@github.com:apache/airflow.git
    # https example: https://github.com/apache/airflow.git
    repo: https://github.com/eiffela65/Airflow-Templates
    branch: main
    rev: HEAD
    depth: 1
    # the number of consecutive failures allowed before aborting
    maxFailures: 0
    # subpath within the repo where dags are located
    # should be "" if dags are at repo root
    subPath: ""
```
Go to the [service account page in google cloud](https://cloud.google.com/kubernetes-engine/docs/tutorials/authenticating-to-cloud-platform), create a key file and download it in your computer. You'll use this
key file to allow pandas access to the raw files stored in GCS.
Create a secret with the service account. For more info you can check the [documetation](https://kubernetes.io/docs/tasks/configmap-secret/managing-secret-using-kubectl/)
```
kubectl create secret generic service-account --from-file=path-to-key-file.json -n airflow
```

Create a secret for connections
```
kubectl create secret generic airflow-connections --from-env-file=secrets.env -n airflow
```

Check the secrets with:
``` shell
kubectl get secrets -n airflow
kubectl describe secrets/airflow-connections -n airflow
```

Create [ConfigMap](https://kubernetes.io/docs/tasks/configure-pod-container/configure-pod-configmap/) for variables 
```
kubectl create configmap airflow-variables --from-env-file=variables.env -n airflow
```

Check the variables with:
``` shell
kubectl describe configmaps airflow-variables -n airflow
kubectl get configmap airflow-variables -o yaml -n airflow
```

Install the airflow chart from the repository:
```
helm install airflow -f airflow-values.yaml apache-airflow/airflow --namespace airflow
```
We can verify that our pods are up and running by executing:
```
kubectl get pods -n airflow
```

### Accessing to Airflow dashboard

The Helm chart shows how to connect:
```
You can now access your dashboard(s) by executing the following command(s) and visiting the corresponding port at localhost in your browser:

Airflow Webserver:     kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
Flower dashboard:      kubectl port-forward svc/airflow-flower 5555:5555 --namespace airflow
Default Webserver (Airflow UI) Login credentials:
    username: admin
    password: admin
Default Postgres connection credentials:
    username: postgres
    password: postgres
    port: 5432

You can get Fernet Key value by running the following:

    echo Fernet Key: $(kubectl get secret --namespace airflow airflow-fernet-key -o jsonpath="{.data.fernet-key}" | base64 --decode)
```
