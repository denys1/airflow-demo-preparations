## airflow-demo-preparations

This is a project developed to demo Apache Airflow capabilities.


## Initialze env
If you want to run the project on your local machine using Docker Containers go through the following steps
Make sure you are using the latest version of `docker` and `docker-compose` 

Clone the project 
```sh
git clone https://github.com/denys1/airflow-demo-preparations.git && cd airflow-demo-preparations
```

## Setup
1. Copy .env.template to .env
2. Fill in your credentials in .env
3. Source the environment variables:
```sh
   source .env
```

## Initialze containers
```sh
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
```

After initialization complete, you should see a message like below.
```
airflow-init_1       | Upgrades done
airflow-init_1       | Admin user airflow created
airflow-init_1       | 2.1.1
start_airflow-init_1 exited with code 0
```


## Running Airflow
Now you can start all services:

```sh
docker-compose up
```

Now let's check whether the containers have been kicked off successfully or not using the following command:
```sh
docker ps
```

You should get a message like this:
```
CONTAINER ID   IMAGE                  COMMAND                  CREATED          STATUS                    PORTS                              NAMES
247ebe6cf87a   apache/airflow:2.1.1   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    8080/tcp                           compose_airflow-worker_1
ed9b09fc84b1   apache/airflow:2.1.1   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    8080/tcp                           compose_airflow-scheduler_1
65ac1da2c219   apache/airflow:2.1.1   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    0.0.0.0:5555->5555/tcp, 8080/tcp   compose_flower_1
7cb1fb603a98   apache/airflow:2.1.1   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    0.0.0.0:8080->8080/tcp             compose_airflow-webserver_1
74f3bbe506eb   postgres:13            "docker-entrypoint.s…"   18 minutes ago   Up 17 minutes (healthy)   5432/tcp                           compose_postgres_1
0bd6576d23cb   redis:latest           "docker-entrypoint.s…"   10 hours ago     Up 17 minutes (healthy)   0.0.0.0:6379->6379/tcp             compose_redis_1
```
Make sure all the containers in `healthy` status.

Do not forget to enable all the `smart_sensor_group_shard_*` DAGs once you have accessed Airflow web interface. 

## Accessing the web interface
The web server is available at: `http://localhost:8080`
The account was created with the default login and password `airflow`:`airflow`. Make sure you change the password after initializing the environment.

## Stopping the environment
To stop the containers run command:
```sh
docker-compose down
```
If you want not only stop containers but also to remove the them, delete volumes with database data etc.
```sh
docker-compose down --volumes --rmi all
```


## Quick commands:

### sensor_dag
To create a Zip archive:
```sh
zip -r packaged_trigger_dag.zip *
```

To create a `run` file on worker:
```sh
docker exec -ti `docker ps | grep worker-2 | cut -d " " -f 1` touch /opt/airflow/run_file_folder/run
```

To list all files on worker's folder:
```sh
docker exec -ti `docker ps | grep worker-2 | cut -d " " -f 1` ls -la /opt/airflow/run_file_folder/
```

### functional_dag
To list all files on worker's folder:
```sh
docker exec -ti `docker ps | grep worker-1 | cut -d " " -f 1` ls -la /opt/airflow/func_dags_data/
```

### Get executor type
```sh
airflow config get-value core executor
```

