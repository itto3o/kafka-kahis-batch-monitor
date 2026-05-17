# E2E 테스트 가이드 — 사육두수 자동 Mark Success 흐름

이 가이드는 사육두수 비교 에러 시나리오에서 Airflow → Spring Boot → Kafka → `KahisServiceImpl` → Airflow Mark Success API(`updateTaskInstancesState`) 까지의 전체 흐름을 로컬에서 검증하는 절차를 정리합니다.

> 운영 SM이 알람을 받고 Airflow UI에서 "Mark as Success"를 누르는 행위를 자동화한 시스템입니다. task 코드를 재실행하는 Clear가 아니라, task instance state만 강제로 success로 전이시키는 방식입니다.

## 0. 사전 준비

- Docker Desktop (Compose v2 포함)
- IDE에서 Spring Boot 실행 가능 (Java 17)
- `docker compose build airflow-init` 한 번 빌드해두면 이후 기동이 빠릅니다

## 1. 인프라 기동

기동 순서가 중요합니다(Oracle init 수동 실행 단계가 있음).

### 1.1 의존 서비스 먼저

```bash
docker compose up -d postgres oracle kafka-first kafka-second kafka-third
```

Postgres init 스크립트가 `batch_monitor` schema와 `airflow` 메타DB를 자동 생성합니다.

```bash
# 확인
docker exec -it kahis-batch-monitor-postgres psql -U postgres -c "\l"
# postgres, airflow DB 존재해야 함
docker exec -it kahis-batch-monitor-postgres psql -U postgres -d postgres -c "\dn"
# batch_monitor schema 존재해야 함
```

### 1.2 Oracle PDB 초기화 (수동)

Oracle은 init 스크립트가 자동 실행되도록 mount되어 있지 않으므로 한 번 수동으로 돌립니다.

```bash
# 기동 완료까지 대기 (수분 소요)
docker compose logs -f oracle | grep -i "DATABASE IS READY"
# Ctrl+C로 빠져나옴

# PDB 생성 (DPL, LSFARM) — 이미 만들어진 PDB가 있으면 OPEN/SAVE 만 수행됨(멱등)
docker exec -i kahis-batch-monitor-oracle sqlplus -s / as sysdba @/scripts/oracle-pdb-init.sql

# 각 PDB에 테이블 + 시나리오 시드
docker exec -i kahis-batch-monitor-oracle sqlplus -s m2msys/m2msys@//localhost:1521/M2MSYS @/scripts/oracle-m2msys-schema.sql
docker exec -i kahis-batch-monitor-oracle sqlplus -s m2msys/m2msys@//localhost:1521/DPL @/scripts/oracle-dpl-schema.sql
docker exec -i kahis-batch-monitor-oracle sqlplus -s m2msys/m2msys@//localhost:1521/LSFARM @/scripts/oracle-lsfarm-schema.sql
```

### 1.3 Airflow 기동

```bash
docker compose up airflow-init      # admin 유저 생성까지 마치고 exit 0
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps
# 모든 서비스가 Up. airflow-webserver는 healthy까지 30초 이상 걸릴 수 있음.
```

접속:
- Airflow UI: <http://localhost:8088> (admin / admin)
- Spring 호출 URL: `http://host.docker.internal:8080/api/v1/errors` (compose에서 자동 주입)

## 2. Spring Boot 실행

IDE에서 `Application` 실행. profile은 `local`(기본).

```
spring.datasource.postgres.url = jdbc:postgresql://localhost:5432/postgres
spring.datasource.oracle.url   = jdbc:oracle:thin:@localhost:1521/DPL
spring.kafka.bootstrap-servers = localhost:9092,localhost:9094,localhost:9096
airflow.api.url                = http://localhost:8088/api/v1   (application-local.yml에서 override됨)
```

> Spring 자체가 8080을 점유하므로 Airflow webserver는 host의 8088로 publish하고, `application-local.yml`에서 `airflow.api.url: http://localhost:8088/api/v1`로 override해두었습니다.

## 3. 시나리오 트리거 및 확인

### 3.1 NORMAL — 자동 Mark Success → downstream 진행

Airflow UI에서 `e2e_make_risk_data` DAG → ▶ Trigger DAG.

기대 흐름:
1. `setup_postgres_tables` 성공
2. `normal.seed_data` 성공 (어제 30 / 오늘 3500 시드)
3. `normal.check_tb_livestock_species_information` **실패** (`ValueError: 20418398의 415002 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: 3500.0, 전일 사육두수: 30.0`)
4. `on_failure_callback`이 Spring으로 POST
5. Spring: `ParserUtil` → `LIVESTOCK_ANOMALY` 라우팅 → Kafka `error.livestock-anomaly` 발행
6. `KafkaLivestockErrorEventConsumer` → `KahisServiceImpl.analysis(event, "20418398", "415002", 3500)`
7. `LivestockHistoryAnalyzer`: HIST `[3500, 3000, 4000, 30]`과 허용범위 `[1750, 7000]` 매칭 → `LIKELY_NORMAL`
8. `StatusLog`에 `AUTO_VERIFIED` 추가 → `AirflowService.markSuccess()` → `updateTaskInstancesState` 호출
9. Airflow가 `normal.check_tb_livestock_species_information` task instance state를 `success`로 전이 (코드 재실행 없이)
10. `include_downstream=true`로 `normal.after_check`도 `upstream_failed` → `success`로 전이 → DAG가 NORMAL 그룹 끝까지 진행
11. Spring `StatusLog`에 `AUTO_MARK_SUCCESS` row 추가

확인 쿼리:

```sql
docker exec -it kahis-batch-monitor-postgres psql -U postgres -d postgres -c "
SELECT create_at, task_id, status_type, judgement_type, substring(reason, 1, 50) AS reason
FROM batch_monitor.status_log
WHERE task_id LIKE 'normal.%'
ORDER BY create_at;"
```

기대 결과 (시간순):
```
RECEIVED          | -             | -
AUTO_VERIFYING    | -             | -
AUTO_VERIFIED     | LIKELY_NORMAL | HIST 정상 — ...
AUTO_MARK_SUCCESS | LIKELY_NORMAL | HIST 정상 — ...
```

Airflow UI에서 `normal.check_tb_livestock_species_information` task가 빨강(failed) → 초록(success)으로 바뀌어 있고, `normal.after_check` task도 성공으로 진행되어 있는 모습을 확인할 수 있어야 합니다.

### 3.2 ANOMALY — 운영자 수동 처리로 종결

같은 DAG run의 `anomaly.*` TaskGroup이 함께 돕니다.

기대 흐름:
1. `anomaly.seed_data` 성공 (어제 50 / 오늘 8000 시드)
2. `anomaly.check_tb_livestock_species_information` 실패 (`00129932의 412002 ...`)
3. Spring → HIST `[88, 90, 92]` 매칭 없음 → `LIKELY_ANOMALY`
4. `StatusLog`에 `MANUAL_REVIEW_REQUIRED` 추가, `AirflowService.markSuccess()` **호출 안 함**
5. `anomaly.check_tb_livestock_species_information` task는 그대로 `failed` 상태, `anomaly.after_check`는 `upstream_failed`로 멈춤

```sql
docker exec -it kahis-batch-monitor-postgres psql -U postgres -d postgres -c "
SELECT create_at, task_id, status_type, judgement_type, substring(reason, 1, 60) AS reason
FROM batch_monitor.status_log
WHERE task_id LIKE 'anomaly.%'
ORDER BY create_at;"
```

기대: `RECEIVED` → `AUTO_VERIFYING` → `MANUAL_REVIEW_REQUIRED` 세 row. `AUTO_MARK_SUCCESS*` 없음.

### 3.3 FAILED — Airflow Mark Success API 호출 실패

NORMAL 시나리오를 실행하되, Spring이 API 호출하는 시점에 Airflow가 응답 불가하도록 만듭니다.

```bash
# Airflow webserver 중단
docker stop kahis-batch-monitor-airflow-webserver

# Spring으로 직접 NORMAL 케이스 호출 (Airflow callback 흉내)
curl -X POST http://localhost:8080/api/v1/errors \
  -H "Content-Type: application/json" \
  -d '{
    "dag_id": "e2e_make_risk_data",
    "task_id": "normal.check_tb_livestock_species_information",
    "dag_run_id": "scheduled__2026-05-10T00:00:00+00:00",
    "error_message": "20418398의 415002 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: 3500.0, 전일 사육두수: 30.0",
    "try_number": 1
  }'

# 검증
docker exec -it kahis-batch-monitor-postgres psql -U postgres -d postgres \
  -c "SELECT create_at, status_type, reason FROM batch_monitor.status_log ORDER BY create_at DESC LIMIT 4;"
# 기대: 마지막 row가 AUTO_MARK_SUCCESS_FAILED

# 정리 (다시 띄움)
docker start kahis-batch-monitor-airflow-webserver
```

## 4. (선택) e2e_copy_oracle_m2msys 실행

Oracle→Postgres 파이프라인 동작 확인용. 사육두수 검증 흐름과는 별개입니다.

Airflow UI에서 `e2e_copy_oracle_m2msys` → Trigger. 성공하면:

```sql
SELECT count(*) FROM m2m_data.tn_mobile_blvstck_hist;
-- M2MSYS 시드와 동일한 row 수가 나와야 함 (현재 oracle-m2msys-schema.sql 기준 7건)
```

## 5. 트러블슈팅

| 증상 | 원인 / 조치 |
|------|-------------|
| Airflow에서 DAG가 안 보임 | `docker compose logs airflow-scheduler` 에서 import error 확인. 보통 `ai_project` import 경로 — `./checks` mount가 `./dags/ai_project`로 매핑되어 있는지 점검 |
| `on_failure_callback` 실패 — Connection refused | Spring이 host에서 떠 있는지 + `host.docker.internal` 가능한지 확인. Linux 호스트라면 `extra_hosts: host-gateway` 동작 확인 |
| Oracle 기동이 5분 넘게 안 끝남 | `docker compose logs oracle \| tail -100` — 보통 첫 기동은 5~10분 걸림. healthcheck 통과 후에 init 스크립트 실행 |
| `AUTO_MARK_SUCCESS`가 안 찍히고 `AUTO_MARK_SUCCESS_FAILED`만 떨어짐 | (a) `application-local.yml`의 `airflow.api.url` 값이 `8088`인지 확인 (b) `AirflowClient`의 `configuration = AirflowClientConfig.class`로 되어 있어 Basic Auth interceptor가 적용되는지 확인 (Airflow API는 admin/admin Basic Auth 필요) |
| Spring 로그에 `401 UNAUTHORIZED` | 위와 동일 — Basic Auth interceptor 미적용. `AirflowClient.configuration` 확인 |
| `Long.parseLong` NumberFormatException | `ParserUtil`의 사육두수 정규식이 정수부만 캡쳐하도록 수정되어 있는지 확인 (`(\d+)(?:\.\d+)?`) |
| race condition으로 `duplicate key value violates unique constraint "pg_type_typname_nsp_index"` | `setup_postgres_tables` task가 `geoai_logs.tb_livestock_species_information`까지 미리 생성하는지 확인 |

## 6. 정리

```bash
docker compose down                 # 컨테이너만 제거 (볼륨 유지)
docker compose down -v              # 볼륨까지 제거 (Oracle/Postgres 데이터 초기화 — Oracle init 다시 필요)
```

다음 검증을 위해 status_log만 비우고 싶다면:
```bash
docker exec -it kahis-batch-monitor-postgres psql -U postgres -d postgres \
  -c "TRUNCATE batch_monitor.status_log;"
```