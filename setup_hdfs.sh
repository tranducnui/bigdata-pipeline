#!/bin/bash
# setup_hdfs.sh — chạy sau khi docker-compose up -d
# Tạo cấu trúc thư mục Medallion trên HDFS và databases trên Hive

echo "=== Waiting for NameNode to be ready... ==="
sleep 10

echo "=== Creating HDFS directory structure ==="
docker exec namenode bash -c "
  # Hive warehouse
  hdfs dfs -mkdir -p /user/hive/warehouse
  hdfs dfs -chmod g+w /user/hive/warehouse

  # Temp
  hdfs dfs -mkdir -p /tmp
  hdfs dfs -chmod 777 /tmp

  # Bronze layer — raw data (immutable)
  hdfs dfs -mkdir -p /data/bronze/batch/sales
  hdfs dfs -mkdir -p /data/bronze/streaming/sales

  # Silver layer — cleaned, validated
  hdfs dfs -mkdir -p /data/silver/customer_spending
  hdfs dfs -mkdir -p /data/silver/sales_realtime

  # Gold layer — aggregated, business-ready
  hdfs dfs -mkdir -p /data/gold/customer_spending
  hdfs dfs -mkdir -p /data/gold/sales_summary

  # Checkpoint cho Spark Streaming
  hdfs dfs -mkdir -p /checkpoint/streaming/sales

  # Set permissions
  hdfs dfs -chmod -R 777 /data
  hdfs dfs -chmod -R 777 /checkpoint

  echo '--- HDFS structure created ---'
  hdfs dfs -ls -R /data
"

echo ""
echo "=== Creating Hive databases ==="
sleep 5

docker exec hiveserver2 bash -c "
  beeline -u jdbc:hive2://hiveserver2:10000 -e \"
    -- Bronze databases
    CREATE DATABASE IF NOT EXISTS bronze;

    -- Silver databases
    CREATE DATABASE IF NOT EXISTS silver;

    -- Gold / Data Mart databases
    CREATE DATABASE IF NOT EXISTS gold;
    CREATE DATABASE IF NOT EXISTS sales_mart;

    SHOW DATABASES;
  \"
"

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Web UIs:"
echo "  HDFS NameNode : http://localhost:9870"
echo "  YARN          : http://localhost:8088"
echo "  HiveServer2   : http://localhost:10002"
echo "  Trino         : http://localhost:8090"
echo "  Airflow       : http://localhost:8081"
echo "  Spark         : http://localhost:8080"
