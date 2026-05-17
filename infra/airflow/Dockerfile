FROM apache/airflow:2.6.2-python3.10

USER root

# Oracle Instant Client는 oracledb thin 모드를 쓰므로 별도 설치 불필요
# curl 등 디버깅 도구만 추가
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt