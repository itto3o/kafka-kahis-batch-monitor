-- Airflow 메타 DB와 사용자 생성
-- postgres 컨테이너 첫 기동 시 한 번만 실행됨
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;