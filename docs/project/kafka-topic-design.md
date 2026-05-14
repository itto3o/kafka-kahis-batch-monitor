# Kafka 토픽 설계서

## 1. Airflow DAG 의존관계 (실제 구조)

```
copy_oracle_m2msys (매일 05:00, 스케줄 트리거)
    │
    │  Oracle(M2MSYS) → PostgreSQL 데이터 적재
    │  - tn_mobile_blvstck (사육두수)
    │  - tn_mobile_blvstck_hist (사육두수 이력)
    │  - tn_mobile_frmhs_info, tn_vhcle_inout, tn_dsnfc_manage_frmhs_info ...
    │  - 행 수 검증 실패 시 ValueError 발생
    │
    ▼
make_risk_data (트리거)
    │
    │  데이터 마트 생성 + 검증 Task들:
    │  ├─ check_tb_livestock_species_information  ← 사육두수 이상감지
    │  ├─ check_tb_prediction_result              ← 예측치 에러
    │  ├─ check_tb_diseasecontrol_status_information
    │  ├─ check_tb_farm_information
    │  ├─ check_tb_car_visit_information
    │  └─ check_calculation_environment_information
    │
    ▼
차량_GPS_위험도 (트리거)
    │
    ├──► geoai_vehicle_risk_view_process (차량 위험도 뷰)
    │
    ▼
fulltime_risk_process (트리거)
    │
    │  전업농 위험도 + 클러스터 분석 + ML 예측
    │
    ▼
region_risk_process (트리거)
    │
    ├──► make_view_report_process_back (화면/보고서)
    │       │
    │       ├──► report_data_qa_test
    │       └──► 방역권역_프로세스 (일일 방역권역)
    │
    └──► ASF_일일프로세스 (ASF 위험도)
            │
            ├──► make_asf_view_process (ASF 화면)
            └──► 실시간_차량_프로세스 (실시간 차량 위험도)


별도 스케줄:
방역권역_정기_데이터_생성 (1월/3월/4월/10월 2일 04:00, 계절 배치)
```

---

## 2. 전체 메시지 흐름도

```
┌─────────────────────────────────────────────────────────────────────┐
│  Airflow DAG (on_failure_callback)                                  │
│                                                                     │
│  에러 발생 시 Spring Boot API 호출 (Push 방식)                        │
└──┬──────────┬──────────┬──────────┬──────────┬──────────────────────┘
   │          │          │          │          │
   ▼          ▼          ▼          ▼          ▼
┌────────┐┌────────┐┌────────┐┌────────┐┌────────┐
│livestock││  hpai  ││predict-││  asf   ││  data  │  ← 에러 유형별 토픽
│-anomaly ││-display││ion     ││-batch  ││  -sync │
│         ││-missing││-anomaly││-failure││-failure│
└───┬─────┘└───┬────┘└───┬────┘└───┬────┘└───┬────┘
    │          │         │         │         │
    └──────────┴─────┬───┴─────────┴─────────┘
                     │
                     ▼
       ┌──────────────────────────┐
       │  에러 유형별 검증 Consumer    │
       │  (VerificationService)    │
       │                          │
       │  ① StatusLog 상태 갱신     │
       │  ② HIST 분석 (가능한 경우)  │
       │  ③ 정상 → Airflow Clear   │
       │     비정상/미지원 →        │
       │     MANUAL_REVIEW_REQUIRED │
       │  ④ error-notification 발행 │
       └──────────┬───────────────┘
                  │
                  ▼
       ┌──────────────────────────┐
       │  error-notification       │ (토픽)
       └──────────┬───────────────┘
                  │
                  ▼
       ┌──────────────────────────┐
       │  NotificationConsumer     │
       │  → SMS 알림 발송           │
       └──────────────────────────┘

  운영자는 SMS를 받고 Airflow UI에서 직접 Clear/Mark Success 처리.
  본 시스템은 운영자 조치 이후의 상태는 추적하지 않음.
```

### 에러 감지 방식: Push (Webhook)

Airflow DAG의 `on_failure_callback`에서 에러 발생 시 **Spring Boot REST API를 직접 호출**합니다.
Spring Boot는 요청을 받아 해당 에러 유형에 맞는 Kafka 토픽으로 발행합니다.

**현재 모든 DAG에 `on_failure_callback`이 없으므로, 대상 Task에 callback을 추가해야 합니다.**

```python
# Airflow DAG 공통 callback 함수
import requests

def on_task_failure(context):
    requests.post("http://spring-boot-host:8080/api/v1/errors", json={
        "dag_id": context["dag"].dag_id,
        "task_id": context["task_instance"].task_id,
        "execution_date": str(context["execution_date"]),
        "error_message": str(context["exception"]),
        "try_number": context["task_instance"].try_number,
    })
```

**callback 추가 대상 Task:**

| DAG | Task ID | 에러 유형 |
|-----|---------|----------|
| `copy_oracle_m2msys` | 각 Oracle→PostgreSQL 적재 Task | `DATA_SYNC_FAILURE` |
| `make_risk_data` | `check_tb_livestock_species_information` | `LIVESTOCK_ANOMALY` |
| `make_risk_data` | `check_tb_prediction_result` | `PREDICTION_ANOMALY` |
| `make_risk_data` | `check_tb_diseasecontrol_status_information` | `DATA_VALIDATION_FAILURE` |
| `make_risk_data` | `check_tb_farm_information` | `DATA_VALIDATION_FAILURE` |
| `make_risk_data` | `check_tb_car_visit_information` | `DATA_VALIDATION_FAILURE` |
| `make_risk_data` | `check_calculation_environment_information` | `DATA_VALIDATION_FAILURE` |
| `ASF_일일프로세스` | 각 ASF 데이터 처리 Task | `ASF_BATCH_FAILURE` |

| 비교 | Pull (폴링) | **Push (채택)** |
|------|------------|-----------------|
| 방식 | Spring이 주기적으로 Airflow API 조회 | Airflow가 에러 시 Spring API 호출 |
| 실시간성 | 낮음 (폴링 주기에 의존) | **높음 (즉시 감지)** |
| Airflow 수정 | 불필요 | **DAG에 callback 추가 필요** |
| 결합도 | Spring → Airflow 단방향 | Airflow → Spring 단방향 |

### 에러 처리 흐름: Spring 파싱 방식

callback은 `task_id + error_message(원문)`만 전송합니다. check 함수는 수정하지 않습니다.
**Spring Boot의 `ErrorReceiveController`에서 `task_id` 기반으로 에러 유형을 판별하고, 에러 메시지를 파싱하여 metadata를 추출합니다.**

```
Airflow callback                Spring Boot                          Kafka
──────────────                 ─────────────                        ─────
task_id +              ──►  1. task_id → 에러 유형 매핑
error_message(raw)          2. 에러 유형별 Parser로 metadata 추출
                            3. 토픽 결정 + 메시지 구성     ──────►  에러 유형별 토픽 발행
```

### task_id → 에러 유형 매핑 (Spring 라우팅 규칙)

| task_id | error_type | 발행 토픽 |
|---------|-----------|----------|
| `check_tb_livestock_species_information` | `LIVESTOCK_ANOMALY` | `error.livestock-anomaly` |
| `check_tb_prediction_result` | `PREDICTION_ANOMALY` | `error.prediction-anomaly` |
| `check_tb_diseasecontrol_status_information` | `DATA_VALIDATION_FAILURE` | `error.data-sync-failure` |
| `check_tb_farm_information` | `DATA_VALIDATION_FAILURE` | `error.data-sync-failure` |
| `check_tb_car_visit_information` | `DATA_VALIDATION_FAILURE` | `error.data-sync-failure` |
| `check_calculation_environment_information` | `DATA_VALIDATION_FAILURE` | `error.data-sync-failure` |
| 그 외 (매핑 없는 task_id) | `UNKNOWN` | `error.data-sync-failure` |

### 에러 메시지 포맷 및 파싱 규칙 (Spring Parser 구현 시 참고)

**1. LIVESTOCK_ANOMALY (사육두수 이상감지)**

에러 메시지 포맷:
```
{farm_serial_no}의 {livestock_species_class_code} 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: {current}, 전일 사육두수: {previous}
```

예시:
```
00293965의 415006 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: 63000.0, 전일 사육두수: 62.0
```

파싱 정규식:
```
^(.+)의\s+(\S+)\s+사육두수 비교에 이상이 감지되었습니다\.\s*당일 사육두수:\s*([\d.]+),\s*전일 사육두수:\s*([\d.]+)$
```

추출 metadata:
```json
{
  "farmNumber": "$1",       // 00293965 (farm_serial_no = M2M.FRMHS_NO)
  "speciesCode": "$2",      // 415006   (livestock_species_class_code = M2M.LSTKSP_CL)
  "currentValue": $3,       // 63000.0  (당일 BRD_HAD_CO)
  "previousValue": $4       // 62.0     (전일 BRD_HAD_CO)
}
```

**2. PREDICTION_ANOMALY (예측치 에러)**

에러 메시지 포맷:
```
{farm_serial_no}의 예측값이 크게 차이납니다. 당일 예측치: {current}, 전일 예측치: {previous}
```

예시:
```
20456536의 예측값이 크게 차이납니다. 당일 예측치: 0.973676, 전일 예측치: 0.816708
```

파싱 정규식:
```
^(.+)의 예측값이 크게 차이납니다\.\s*당일 예측치:\s*([\d.]+),\s*전일 예측치:\s*([\d.]+)$
```

추출 metadata:
```json
{
  "farmSerialNo": "$1",         // 20456536
  "currentPrediction": $2,      // 0.973676
  "previousPrediction": $3      // 0.816708
}
```

**3. DATA_VALIDATION_FAILURE (데이터 검증 실패)**

에러 메시지 포맷이 check 함수마다 다릅니다. 파싱 실패 시 error_message 원문을 그대로 보존합니다.

| task_id | 에러 메시지 예시 |
|---------|----------------|
| `check_tb_diseasecontrol_status_information` | `농장 정보의 개수가 한달 평균 개수와 10% 이상 차이납니다. 확인이 필요합니다.` |
| `check_tb_farm_information` | `PNU 시도 코드가 표준 코드와 다른 농장이 존재합니다. 확인이 필요합니다. PNU : {code}` |
| `check_tb_farm_information` | `전날과 농장 정보의 갯수 차이가 오차범위 이상입니다. 확인이 필요합니다.` |

> DATA_VALIDATION_FAILURE는 메시지가 단순하고 파싱할 수치 데이터가 적으므로, metadata 없이 error_message 원문만 전달해도 운영에 충분합니다.

**4. 파싱 실패 시 처리**

정규식 매칭이 실패해도 에러 이벤트는 **반드시 발행**합니다.
metadata가 비어있을 뿐, error_message 원문은 보존됩니다.

```json
{
  "eventId": "uuid",
  "dagId": "make_risk_data",
  "taskId": "check_tb_livestock_species_information",
  "errorType": "LIVESTOCK_ANOMALY",
  "errorMessage": "(원문 그대로)",
  "metadata": {}
}
```

---

## 3. 토픽 목록

### 3.1 에러 감지 토픽 (현재 ErrorType enum 기준 — 9개)

각 토픽은 `ErrorType` enum의 `topic` 필드로부터 결정되며, `isNeedAnalysis` 플래그에 따라 Consumer가 분기됩니다.

| 토픽 이름 | 에러 유형 | `isNeedAnalysis` | 발생 위치 (DAG / Task 예시) | 처리 Consumer |
|-----------|----------|------------------|---------------------------|--------------|
| `error.livestock-anomaly` | `LIVESTOCK_ANOMALY` (사육두수 이상감지) | true | `make_risk_data` / `check_tb_livestock_species_information` | `KafkaLivestockErrorEventConsumer` |
| `error.prediction-anomaly` | `PREDICTION_ANOMALY` (예측치 에러) | false | `make_risk_data` / `check_tb_prediction_result` | `KafkaEventConsumer` |
| `error.farm-count-anomaly` | `FARM_COUNT_ANOMALY` (농장 개수 이상) | false | `check_tb_diseasecontrol_status_information`, `check_tb_farm_information` | `KafkaEventConsumer` |
| `error.pnu-anomaly` | `PNU_ANOMALY` (PNU 시도 코드 불일치) | false | `check_tb_farm_information` | `KafkaEventConsumer` |
| `error.farm-coordinate-missing` | `FARM_COORDINATE_MISSING` (좌표 누락) | false | `check_tb_farm_information` | `KafkaEventConsumer` |
| `error.data-not-found` | `DATA_NOT_FOUND` (데이터/방역카드/농장정보/축종정보 부재) | false | 다수 check 함수 | `KafkaEventConsumer` |
| `error.trainingset-count-mismatch` | `TRAININGSET_COUNT_MISMATCH` | false | `check_tb_prediction_result` | `KafkaEventConsumer` |
| `error.calc-env-anomaly` | `CALC_ENV_ANOMALY` (계산 환경 ratio 이상) | false | `check_calculation_environment_information` | `KafkaEventConsumer` |
| `error.unknown` | `UNKNOWN` (파서 매칭 실패 / 일반 예외) | false | `Error occurred:` 패턴 등 | `KafkaEventConsumer` |

> 현재 enum에는 이전 설계의 `HPAI_DISPLAY_MISSING`, `ASF_BATCH_FAILURE`, `DATA_SYNC_FAILURE`가 없습니다. 추후 필요 시 enum에 추가하면 토픽도 자동 산출됩니다.

### 3.2 공통 처리 토픽 (TODO)

| 토픽 이름 | 용도 | Producer | Consumer |
|-----------|------|----------|----------|
| `error-notification` (미구현) | SMS 등 알림 발송 대상 | 각 Consumer가 후속 발행 예정 | `NotificationConsumer` (TODO) |

> 운영자 승인 워크플로우를 시스템 내부에 두지 않으므로 `error-review-pending` / `batch-action-request` 토픽은 사용하지 않습니다. 자동 판단이 정상이면 `AirflowService`가 Airflow `clearTaskInstances` API를 직접 호출하고, 그 외에는 운영자가 Airflow UI에서 직접 처리합니다.

### 3.3 설계 결정 사항

**Q. 왜 에러 유형별로 토픽을 나누는가?**
- 유형별로 검증 로직이 다름 (사육두수는 HIST 조회, 예측치는 전일 비교, ASF는 선행 배치 확인, 데이터 적재는 행 수 비교)
- 유형별 독립적인 Consumer를 두어 장애 격리 가능
- 특정 유형만 처리량이 몰릴 때 해당 토픽의 파티션만 확장 가능

**Q. check 함수마다 토픽이 따로 있나?**
- 에러 메시지 정규식 매칭 결과(`ParserUtil`의 `ErrorType` 분류)에 따라 토픽이 결정됩니다.
- `check_tb_farm_information` 한 task에서도 메시지에 따라 `PNU_ANOMALY` / `FARM_COORDINATE_MISSING` / `FARM_COUNT_ANOMALY` 등 다른 토픽으로 갈릴 수 있습니다.
- 매칭 실패 시 `UNKNOWN` 토픽으로 라우팅.

**Q. 운영자 승인 토픽(`error-review-pending`)을 두지 않는 이유는?**
- 운영자가 Airflow UI에서 직접 조치하므로, Spring 측에 별도 승인 화면/큐가 불필요
- 자동 Clear 가능 케이스만 시스템이 처리하고 나머지는 사람에게 위임 — 책임 경계를 단순화

---

## 4. 메시지 스키마

### 4.1 에러 감지 토픽 — 현재 구현 스키마

Airflow `on_failure_callback`에서 Spring API(`POST /api/v1/errors`, JSON)를 거쳐 발행되는 `KafkaEvent` (record).

- **Key**: `{dagId}-{taskId}-{yyyy-MM-dd}` (`KafkaEventProducer`에서 `LocalDateTime.now().toLocalDate()` 기준 생성)
- **Value** (`KafkaEvent` record 직렬화):

```json
{
  "eventId": "check_tb_livestock_species_information-123456789",
  "dagId": "make_risk_data",
  "taskId": "check_tb_livestock_species_information",
  "errorType": "LIVESTOCK_ANOMALY",
  "errorMessage": "00293965의 415006 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: 63000.0, 전일 사육두수: 62.0",
  "metadata": {
    "farmNumber": "00293965",
    "speciesCode": "415006",
    "currentValue": "63000.0",
    "previousValue": "62.0"
  },
  "occurredAt": "2026-04-08T09:30:00"
}
```

> `eventId`는 현재 `taskId + "-" + LocalDateTime.now().getNano()` 형식 (`StatusLogServiceImpl`). 동일 nano 충돌 가능성이 있어 향후 결정적 키/UUID 도입 후보.
> `metadata` 값은 모두 문자열입니다 (`Map<String, String>`).

**토픽별 errorType 매핑 (현재 ErrorType enum 기준):**

| 토픽 | errorType | `isNeedAnalysis` | 비고 |
|------|-----------|------------------|------|
| `error.livestock-anomaly` | `LIVESTOCK_ANOMALY` | true | `KafkaLivestockErrorEventConsumer` 전담 |
| `error.prediction-anomaly` | `PREDICTION_ANOMALY` | false | `KafkaEventConsumer` 묶음 |
| `error.farm-count-anomaly` | `FARM_COUNT_ANOMALY` | false | `KafkaEventConsumer` 묶음 |
| `error.pnu-anomaly` | `PNU_ANOMALY` | false | `KafkaEventConsumer` 묶음 |
| `error.farm-coordinate-missing` | `FARM_COORDINATE_MISSING` | false | `KafkaEventConsumer` 묶음 |
| `error.data-not-found` | `DATA_NOT_FOUND` | false | `KafkaEventConsumer` 묶음 |
| `error.trainingset-count-mismatch` | `TRAININGSET_COUNT_MISMATCH` | false | `KafkaEventConsumer` 묶음 |
| `error.calc-env-anomaly` | `CALC_ENV_ANOMALY` | false | `KafkaEventConsumer` 묶음 |
| `error.unknown` | `UNKNOWN` | false | 파서 매칭 실패 시 라우팅 |

**에러 유형별 metadata 예시 (현재 ParserUtil이 추출하는 키):**

모든 값은 문자열입니다 (`Map<String, String>`).

```json
// LIVESTOCK_ANOMALY
"metadata": {
  "farmNumber": "00293965",
  "speciesCode": "415006",
  "currentValue": "63000.0",
  "previousValue": "62.0"
}

// PREDICTION_ANOMALY
"metadata": {
  "farmNumber": "20456536",
  "currentPrediction": "0.973676",
  "previousPrediction": "0.816708"
}

// PNU_ANOMALY
"metadata": { "pnuCode": "..." }

// FARM_COORDINATE_MISSING
"metadata": { "missingFarmCount": "3", "farmList": "..." }

// DATA_NOT_FOUND
"metadata": { "standardDate": "2026-03-03", "resource": "축종정보" }

// TRAININGSET_COUNT_MISMATCH
"metadata": { "standardDate": "2026-03-03" }

// FARM_COUNT_ANOMALY / CALC_ENV_ANOMALY → metadata: {}  (정규식 그룹 없음)

// UNKNOWN — "Error occurred: {e}" 패턴 매칭 시
"metadata": { "exception": "..." }

// 그 외 모든 매칭 실패 → metadata: {}, errorMessage 원문만 보존
```

### 4.2 `error-notification` (TODO)

알림 발송 대상 이벤트. 현재는 토픽/Producer/Consumer 모두 구현되지 않았으며, 도입 시 스키마 합의 예정.

---

## 5. 토픽 설정 (제안)

> 현재 코드는 토픽 자동 생성에 의존하고 별도 운영 설정이 없는 상태입니다. 아래는 도입 시 권장값.

### 5.1 에러 감지 토픽

| 토픽 | 권장 파티션 수 | Replication Factor | Retention | 비고 |
|------|--------------|-------------------|-----------|------|
| `error.livestock-anomaly` | 3 | 3 | 7일 | 가장 빈번한 에러, 분석 부하도 큼 |
| `error.prediction-anomaly` | 1 | 3 | 7일 | Task 자체는 진행, 처리량 낮음 |
| `error.farm-count-anomaly` | 1 | 3 | 7일 | |
| `error.pnu-anomaly` | 1 | 3 | 7일 | |
| `error.farm-coordinate-missing` | 1 | 3 | 7일 | |
| `error.data-not-found` | 1 | 3 | 7일 | |
| `error.trainingset-count-mismatch` | 1 | 3 | 7일 | |
| `error.calc-env-anomaly` | 1 | 3 | 7일 | |
| `error.unknown` | 1 | 3 | 14일 | 파서 매칭 실패 — 디버깅 위해 보관기간 길게 |

### 5.2 공통 처리 토픽 (TODO)

| 토픽 | 권장 파티션 수 | Replication Factor | Retention | 비고 |
|------|--------------|-------------------|-----------|------|
| `error-notification` (미구현) | 1 | 3 | 3일 | 알림은 순서 보장, 단기 보관 |

- **Replication Factor 3**: 3-node 클러스터이므로 모든 노드에 복제하여 고가용성 확보
- **Retention**: 이력은 DB에 저장하므로 토픽 보관은 단기로 설정

---

## 6. 자동 처리 범위 및 안전 장치

### 6.1 현재 자동 처리 정책

`ErrorType.isNeedAnalysis` 플래그로 분석 가능 여부를 결정하고, `KafkaEventConsumer`가 `notAnalysisTopics()`를 자동 산출해 한 번에 구독합니다.

| errorType | `isNeedAnalysis` | 현재 동작 | 확대 조건 |
|-----------|------------------|----------|----------|
| `LIVESTOCK_ANOMALY` | true | HIST 조회 + tolerance 분석 → 정상 시 `AUTO_CLEARED` → `AirflowService.clear()` → `AUTO_CLEAR_SUCCESS`/`AUTO_CLEAR_FAILED`. 비정상/판단불가 시 `MANUAL_REVIEW_REQUIRED` | — |
| 그 외 모든 errorType | false | 즉시 `MANUAL_REVIEW_REQUIRED` + `JudgementType.UNKNOWN` | 유형별 자동 판단 로직 구현 시 enum 플래그 토글 + 전용 Consumer 추가 |

### 6.2 안전 장치 (현재/계획)

> **제약**: 사육두수는 위험도에 직결되므로, 잘못된 데이터가 자동 패스되면 안 됨 (REQUIREMENTS.md 6.1)

- **현재**: `LIVESTOCK_ANOMALY` 분석에서 정상 판단 시 `AUTO_CLEARED` 이력을 남기고 `AirflowService.clear()`로 Airflow `clearTaskInstances` API를 호출. 응답에 따라 `AUTO_CLEAR_SUCCESS`(`taskInstances` 1건 이상) 또는 `AUTO_CLEAR_FAILED`(Feign 예외 또는 빈 `taskInstances`)로 종결. 요청 시 `only_failed=true`, `include_downstream=true`, `reset_dag_runs=true`로 downstream task와 DAG run까지 함께 재개됨.
- **현재**: 모든 종결 결과(`AUTO_CLEAR_SUCCESS` / `AUTO_CLEAR_FAILED` / `MANUAL_REVIEW_REQUIRED`)는 `StatusLog`에 append-only로 영구 보관 → 사후 정확도 측정 가능.
- **계획**: SMS 알림(`error-notification` 토픽 + `NotificationConsumer`) 도입 시 자동 처리 케이스에도 운영자 인지를 위해 항상 알림 발송.

---

## 7. 사육두수 검증 시 참조 테이블/컬럼 매핑

### 7.1 사육두수(BRD_HAD_CO / BRD_CO) 컬럼 위치

| DB | 테이블 | 컬럼 | 타입 | 용도 |
|----|--------|------|------|------|
| M2M | `TN_MOBILE_BLVSTCK` | `BRD_HAD_CO` | NUMBER(12,0) | 현재 사육두수 |
| M2M | `TN_MOBILE_BLVSTCK_HIST` | `BRD_HAD_CO` | NUMBER(12,0) | 사육두수 변경 이력 (PK: FRMHS_NO + LSTKSP_CL + CHANGE_DT) |
| DPL | `TN_BLVSTCK` | `BRD_HAD_CO` | NUMBER(12,0) | 마스터 사육두수 |
| LSFARM | `EAI_TN_FARM_SCALE` | `BRD_CO` | NUMBER | 원천 사육두수 (축종별) |
| LSFARM | `EAI_TN_FARM_SCALE_DETAIL` | `BRD_CO` | NUMBER | 원천 사육두수 상세 (품종별) |

### 7.2 농가번호 체계 (DB 간 매핑 경로)

농가번호가 DB마다 다르므로 교차 검증 시 매핑이 필요합니다.

```
M2M / DPL                         LSFARM
─────────                         ──────
FRMHS_NO (CHAR 8)                 FRMHS_SN (NUMBER 13) ← TN_FRMHS.FRMHS_SN
  │                                   │
  │  M2M.TN_MOBILE_FRMHS_INFO        │  TN_FARMHS_NO_MAPNG
  │  .FDB_FRMHS_SN                    │  .FRMHS_SN → .CNTC_FRMHS_NO
  │         │                         │
  └─────────┼─────────────────────────┘
            │
            ▼
    DPL.VIEW_FRMHS.FDB_FRMHS_SN (브릿지)
            │
            ▼
    LSFARM.EAI_TN_FARM_INFO.FARM_NO
    LSFARM.EAI_TN_FARM_SCALE.FARM_NO → BRD_CO (원천 사육두수)
    LSFARM.EAI_TN_FARM_SCALE_DETAIL.FARM_NO → BRD_CO (상세)
```

### 7.3 자동 검증 시 조회 순서 (현재 구현)

`KafkaLivestockErrorEventConsumer` → `KahisServiceImpl.analysis()` 흐름:

```
1단계: HIST 조회 + tolerance 분석 (LivestockHistoryAnalyzer)
   FarmMapper.selectMobileBreedingLivestockHistory(farmId, speciesCode)
   → 12개월 이내, ORDER BY LAST_CHANGE_DT DESC
   → null/lastChangeDt null 제거 + Java 측 명시 정렬
   → currentValue × [0.5, 2.0] 매칭값 존재 여부로 LIKELY_NORMAL / LIKELY_ANOMALY
   → 매칭값 0건이면서 history 자체도 비어 있으면 UNKNOWN

2단계: 방역본부 농가번호 매핑 (LsFarmIdFinder, 부가 정보)
   FarmMapper.selectFarmIdDpl(farmId)        → DPL FRMHS_NO
   FarmMapper.selectFarmIdLsfarm(dplFarmId)  → LSFARM CNTC_FRMHS_NO
   → StatusLog.lsfarmId 컬럼에 함께 적재
```

`FarmMapper`에는 추가로 `selectFarmInfo`, `selectFarmScale`, `selectFarmScaleDetail`이 정의되어 있어 향후 운영자 판단 지원 정보로 확장 가능.

> **참고**: DPL/LSFARM 조회는 Oracle DB 접근이 필요하므로 폐쇄망 내 방화벽 오픈이 선행되어야 합니다. 미오픈 시 `LsFarmIdFinder`가 NPE 또는 빈 결과로 실패할 수 있습니다.

---

## 8. 향후 확장 토픽

| 토픽 (후보) | 시점 | 용도 |
|-------------|------|------|
| `batch-action-result` | 조치 결과 추적 필요 시 | Clear/Success 실행 결과 (성공/실패) |
| `data-sync-detected` | 원천 DB Polling 구현 시 | 방역본부 데이터 수정 감지 이벤트 |
---

## 9. checks 에러 메시지 추출 결과

- `"{std_dt}의 데이터가 존재하지 않습니다."` (`check_calculation_environment_information.py`)
- `"농장 정보의 개수가 한달 평균 개수와 10% 이상 차이납니다. 확인이 필요합니다."` (`check_tb_diseasecontrol_status_information.py`)
- `"{std_dt}의 방역카드 정보가 존재하지 않습니다."` (`check_tb_diseasecontrol_status_information.py`)
- `"{std_dt}의 농장정보가 존재하지 않습니다."` (`check_tb_farm_information.py`)
- `"좌표 정보가 없는 농장이 존재합니다. {len(loc_null_lst)}개  농장 : {loc_null_lst_formatting}"` (`check_tb_farm_information.py`)
- `"PNU 시도 코드가 표준 코드와 다른 농장이 존재합니다. 확인이 필요합니다. PNU : {i}"` (`check_tb_farm_information.py`)
- `"전날과 농장 정보의 갯수 차이가 오차범위 이상입니다. 확인이 필요합니다."` (`check_tb_farm_information.py`)
- `"{std_dt}의 축종정보가 존재하지 않습니다."` (`check_tb_livestock_species_information.py`)
- `"{row['farm_serial_no']}의 {row['livestock_species_class_code']} 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: {row['present_breeding_livestock_count']}, 전일 사육두수: {row['present_breeding_livestock_count_prev']}"` (`check_tb_livestock_species_information.py`)
- `"{ standard_date }의 tb_prediction_result 데이터수가 tb_trainingset의 데이터수와 일치하지 않습니다."` (`check_tb_prediction_result.py`)
- `"{row['farm_serial_no']}의 예측값이 크게 차이납니다. 당일 예측치: {row['infection_risk_rank']}, 전일 예측치: {row['infection_risk_rank_prev']}"` (`check_tb_prediction_result.py`)
- 공통 예외 로그 메시지: `Error occurred: {e}` (`check_calculation_environment_information.py`, `check_tb_diseasecontrol_status_information.py`, `check_tb_farm_information.py`, `check_tb_livestock_species_information.py`)
