docker-compose up -d
timeout /t 15
docker exec -it airflow cat /opt/airflow/standalone_admin_password.txt