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

### 3.1 에러 감지 토픽 (에러 유형별 분리)

| 토픽 이름 | 에러 유형 | 발생 위치 (DAG / Task) | Consumer |
|-----------|----------|----------------------|----------|
| `error.livestock-anomaly` | 사육두수 이상감지 | `make_risk_data` / `check_tb_livestock_species_information` | LivestockVerificationService |
| `error.hpai-display-missing` | 확진농가 HPAI 화면 표출 누락 | 수동 등록 (에러가 발생하지 않는 유형) | HpaiVerificationService |
| `error.prediction-anomaly` | 예측치 에러 | `make_risk_data` / `check_tb_prediction_result` | PredictionVerificationService |
| `error.asf-batch-failure` | ASF 배치 에러 | `ASF_일일프로세스` / 각 데이터 처리 Task | AsfVerificationService |
| `error.data-sync-failure` | 데이터 적재 실패 | `copy_oracle_m2msys` / Oracle→PG 적재 Task | DataSyncVerificationService |

> **참고**: `error.hpai-display-missing`은 Airflow에서 에러가 발생하지 않는 유형입니다.
> 운영자가 직접 발견 후 REST API를 통해 수동으로 이벤트를 등록합니다.

### 3.2 공통 처리 토픽

| 토픽 이름 | 용도 | Producer | Consumer |
|-----------|------|----------|----------|
| `error-notification` | SMS 등 알림 발송 대상 | 각 VerificationConsumer | NotificationService |

> 운영자 승인 워크플로우를 시스템 내부에 두지 않으므로 `error-review-pending` / `batch-action-request` 토픽은 사용하지 않습니다. 자동 Clear가 가능하면 Verification Consumer가 직접 Airflow API를 호출하고, 그 외에는 운영자가 Airflow UI에서 직접 처리합니다.

### 3.3 설계 결정 사항

**Q. 왜 에러 유형별로 토픽을 나누는가?**
- 유형별로 검증 로직이 다름 (사육두수는 HIST 조회, 예측치는 전일 비교, ASF는 선행 배치 확인, 데이터 적재는 행 수 비교)
- 유형별 독립적인 Consumer를 두어 장애 격리 가능
- 특정 유형만 처리량이 몰릴 때 해당 토픽의 파티션만 확장 가능

**Q. `error.data-sync-failure`를 추가한 이유는?**
- `copy_oracle_m2msys`에서 Oracle↔PostgreSQL 행 수 불일치 시 ValueError 발생
- 이 에러가 해결되지 않으면 하위 전체 DAG 체인(`make_risk_data` → ... → `실시간_차량_프로세스`)이 실행되지 않음
- 빠른 감지와 알림이 필요

**Q. `make_risk_data`의 나머지 check Task들은?**
- `check_tb_diseasecontrol_status_information`, `check_tb_farm_information`, `check_tb_car_visit_information`, `check_calculation_environment_information`
- 현재는 빈번하지 않으므로 별도 토픽 없이 `error.data-sync-failure`로 통합 라우팅
- 빈도가 증가하면 별도 토픽으로 분리 가능

**Q. 운영자 승인 토픽(`error-review-pending`)을 두지 않는 이유는?**
- 운영자가 Airflow UI에서 직접 조치하므로, Spring 측에 별도 승인 화면/큐가 불필요
- 자동 Clear 가능 케이스만 시스템이 처리하고 나머지는 사람에게 위임 — 책임 경계를 단순화

---

## 4. 메시지 스키마

### 4.1 에러 감지 토픽 (5개 공통 포맷)

Airflow `on_failure_callback`에서 Spring API를 거쳐 발행되는 에러 이벤트.

- **Key**: `{dag_id}_{task_id}_{execution_date}` (중복 감지 및 파티션 분배용)
- **Value**:

```json
{
  "eventId": "uuid",
  "dagId": "make_risk_data",
  "taskId": "check_tb_livestock_species_information",
  "executionDate": "2026-04-08",
  "errorType": "LIVESTOCK_ANOMALY",
  "errorMessage": "00293965의 415006 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: 63000.0, 전일 사육두수: 62.0",
  "detectedAt": "2026-04-08 09:30:00",
  "tryNumber": 1,
  "metadata": {
    "farmNumber": "00293965",
    "currentValue": 63000.0,
    "previousValue": 62.0
  }
}
```

**토픽별 errorType 및 발생 위치 매핑:**

| 토픽 | errorType | 발생 DAG | 발생 Task |
|------|-----------|---------|----------|
| `error.livestock-anomaly` | `LIVESTOCK_ANOMALY` | `make_risk_data` | `check_tb_livestock_species_information` |
| `error.hpai-display-missing` | `HPAI_DISPLAY_MISSING` | - (수동 등록) | - |
| `error.prediction-anomaly` | `PREDICTION_ANOMALY` | `make_risk_data` | `check_tb_prediction_result` |
| `error.asf-batch-failure` | `ASF_BATCH_FAILURE` | `ASF_일일프로세스` | 각 데이터 처리 Task |
| `error.data-sync-failure` | `DATA_SYNC_FAILURE` | `copy_oracle_m2msys` | Oracle→PG 적재 Task |

**에러 유형별 metadata 예시:**

```json
// LIVESTOCK_ANOMALY - 사육두수 이상감지
// 검증 대상 컬럼: M2M.BRD_HAD_CO / DPL.BRD_HAD_CO / LSFARM.BRD_CO
"metadata": {
  "farmNumber": "00293965",
  "speciesCode": "415006",
  "speciesName": "산란계(육용)",
  "currentValue": 63000.0,
  "previousValue": 62.0
}

// PREDICTION_ANOMALY - 예측치 에러
"metadata": {
  "targetId": "20456536",
  "currentPrediction": 0.973676,
  "previousPrediction": 0.816708
}

// ASF_BATCH_FAILURE - ASF 배치 에러
"metadata": {
  "failedTask": "sp_insert_tb_mother_pig_information",
  "upstreamDag": "make_risk_data",
  "upstreamStatus": "failed"
}

// DATA_SYNC_FAILURE - 데이터 적재 실패
"metadata": {
  "tableName": "tn_mobile_blvstck_hist",
  "sourceCount": 150000,
  "targetCount": 149500,
  "sourceDb": "M2MSYS (Oracle)"
}

// HPAI_DISPLAY_MISSING - 수동 등록
"metadata": {
  "farmNumber": "00123456",
  "description": "확진농가 HPAI 화면 미표출"
}
```

### 4.2 `error-notification`

알림 발송 대상 이벤트.

- **Key**: `{eventId}`
- **Value**:

```json
{
  "eventId": "uuid",
  "notificationType": "SMS",
  "recipients": ["010-XXXX-XXXX"],
  "title": "[배치에러] make_risk_data / 사육두수 이상감지",
  "message": "농가번호: 00293965, 당일: 63000.0, 전일: 62.0. Airflow 확인 필요.",
  "severity": "HIGH",
  "createdAt": "2026-04-08 09:30:00"
}
```

---

## 5. 토픽 설정

### 5.1 에러 감지 토픽

| 토픽 | 파티션 수 | Replication Factor | Retention | 비고 |
|------|----------|-------------------|-----------|------|
| `error.livestock-anomaly` | 3 | 3 | 7일 | 가장 빈번한 에러 |
| `error.hpai-display-missing` | 1 | 3 | 7일 | 수동 등록, 발생 빈도 낮음 |
| `error.prediction-anomaly` | 1 | 3 | 7일 | Task 자체는 진행되므로 낮은 처리량 |
| `error.asf-batch-failure` | 1 | 3 | 7일 | 사육두수 연쇄 영향으로 발생 |
| `error.data-sync-failure` | 3 | 3 | 7일 | 적재 테이블이 다수이므로 파티션 분배 |

### 5.2 공통 처리 토픽

| 토픽 | 파티션 수 | Replication Factor | Retention | 비고 |
|------|----------|-------------------|-----------|------|
| `error-notification` | 1 | 3 | 3일 | 알림은 순서 보장, 단기 보관 |

- **Replication Factor 3**: 3-node 클러스터이므로 모든 노드에 복제하여 고가용성 확보
- **Retention**: 이력은 DB에 저장하므로 토픽 보관은 단기로 설정

---

## 6. 자동 처리 범위 및 안전 장치

### 6.1 현재 자동 처리 정책

errorType 단위로 "자동 Clear 가능 / 운영자 수동 처리"를 결정합니다. 운영 안정화 정도에 따라 자동 처리 가능 errorType을 단계적으로 확대합니다.

| errorType | 현재 정책 | 확대 조건 |
|-----------|----------|----------|
| `LIVESTOCK_ANOMALY` | 자동 Clear (HIST 정상 판단 시) | - |
| `PREDICTION_ANOMALY` | 운영자 수동 처리 | 자동 판단 로직 합의 후 |
| `ASF_BATCH_FAILURE` | 운영자 수동 처리 | 선행 배치 자동 복구 가능해진 후 |
| `DATA_SYNC_FAILURE` | 운영자 수동 처리 | 원천 DB 정합성 자동 검증 가능해진 후 |
| `HPAI_DISPLAY_MISSING` | 운영자 수동 처리 | (수동 등록 유형) |

### 6.2 안전 장치

> **제약**: 사육두수는 위험도에 직결되므로, 잘못된 데이터가 자동 패스되면 안 됨

- 자동 Clear 호출은 errorType 단위로 ON/OFF 가능 (설정값으로 토글)
- 자동 Clear 결과(`AUTO_CLEARED` / `AUTO_CLEAR_FAILED`)는 `StatusLog`에 영구 보관 → 사후 정확도 측정
- Airflow Clear API 호출은 1회만 시도 (실패 시 운영자 수동 개입 — `AUTO_CLEAR_FAILED` 상태로 알림)
- 자동 처리되었더라도 운영자에게는 알림(SMS)을 항상 발송하여 사후 인지 가능

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

### 7.3 자동 검증 시 조회 순서

`LivestockVerificationService`가 사육두수 이상감지 이벤트를 받으면:

```
1단계: M2M HIST 조회 (자동)
   SELECT BRD_HAD_CO, CHANGE_DT
   FROM TN_MOBILE_BLVSTCK_HIST
   WHERE FRMHS_NO = '{farmNumber}'
     AND LSTKSP_CL = '{speciesCode}'
   ORDER BY CHANGE_DT DESC
   → 근 1년 이력으로 autoJudgement 판단

2단계: 이상 판단 시 DPL/LSFARM 교차 검증 (운영자 판단 지원 정보)
   DPL.TN_BLVSTCK.BRD_HAD_CO 조회
   → LSFARM 농가번호 매핑 (TN_FARMHS_NO_MAPNG, INSTT_SE_CODE='03')
   → LSFARM.EAI_TN_FARM_SCALE.BRD_CO 조회
   → 결과를 verification.crossDbRecords에 포함하여 운영자에게 제시
```

> **참고**: 2단계의 DPL/LSFARM 조회는 Oracle DB 접근이 필요하므로,
> 폐쇄망 내 Spring Boot ↔ Oracle DB 간 방화벽 오픈이 선행되어야 합니다.
> 방화벽 미오픈 시 1단계(M2M HIST) 조회만으로 운영하고, 2단계는 운영자가 직접 수행합니다.

---

## 8. 향후 확장 토픽

| 토픽 (후보) | 시점 | 용도 |
|-------------|------|------|
| `batch-action-result` | 조치 결과 추적 필요 시 | Clear/Success 실행 결과 (성공/실패) |
| `data-sync-detected` | 원천 DB Polling 구현 시 | 방역본부 데이터 수정 감지 이벤트 |