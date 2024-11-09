#! /bin/bash


docker exec -ti `docker ps | grep worker-1 | cut -d " " -f 1` mkdir /opt/airflow/func_dags_data
docker exec -ti `docker ps | grep worker-2 | cut -d " " -f 1` mkdir /opt/airflow/run_file_folder
echo '-- Created folders --'


webserver_container_id=$(docker ps | grep webserver | cut -d " " -f 1)
docker exec -ti $webserver_container_id airflow connections add --conn-type 'fs' 'custom_fs_connection' --conn-extra '{ "path": "/opt/airflow/run_file_folder/" }'
docker exec -ti $webserver_container_id airflow connections add --conn-type 'postgres' 'postgres_default' --conn-host 'postgres' --conn-login 'airflow' --conn-password 'airflow' --conn-port '5432'
echo '-- Created connections --'


source install/tokens.ini
vault_container_id=$(docker ps | grep vault | cut -d " " -f 1)
docker exec -ti $vault_container_id vault login $vault_token
docker exec -ti $vault_container_id vault secrets enable -path=airflow -version=2 kv
docker exec -ti $vault_container_id vault kv put airflow/variables/slack_token value=$slack_token
echo '-- Added token --'

echo '--- Install Completed --- '