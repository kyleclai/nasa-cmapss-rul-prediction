-- Creates all databases and users needed by the stack.
-- Runs automatically on first postgres container startup.

-- Airflow metadata database
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow_db OWNER airflow;

-- Data warehouse database
CREATE USER warehouse WITH PASSWORD 'warehouse';
CREATE DATABASE warehouse_db OWNER warehouse;

-- Metabase internal database
CREATE USER metabase WITH PASSWORD 'metabase';
CREATE DATABASE metabase_db OWNER metabase;
