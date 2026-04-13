# 요구사항 정의서

## 1. 프로젝트 개요

Airflow 기반 배치 작업에서 발생하는 장애를 Kafka + Spring Boot로 감지하고, 데이터 검증 후 반자동/자동 복구를 지원하는 시스템입니다.

> **핵심 원칙**: 단순 Clear 재실행으로 해결되는 케이스는 거의 없으며, **데이터 검증과 조치가 반드시 선행**되어야 합니다.

---

## 2. 장애 유형 및 우선순위

### 2.1 Top 에러 케이스

| 순위 | 에러 유형 | DAG / Task | 특징 |
|------|----------|------------|------|
| 1 | 사육두수 이상감지 | `make_risk_data` / `check_tb_livestock_species_information` | 데이터 조치 후 Success 또는 Clear 필요. 토요일 배치에 걸리면 ASF 에러로 전파됨 |
| 2 | 확진농가 HPAI 화면 표출 누락 | - | 에러가 발생하지 않으나 데이터를 찾아서 삭제/수정 후 해당 날짜의 DAG Task 재배치 필요 |
| 3 | 예측치 에러 | `make_risk_data` / `check_tb_prediction_result` | Task 자체는 진행됨. 데이터 확인 후 Success 처리만 하면 됨 |
| 4 | ASF 배치 에러 | ASF 관련 DAG | 전날 데이터 배치가 미완료되어 발생 (사육두수 이상감지의 연쇄 영향) |

### 2.2 자주 수동 처리하는 DAG/Task

| DAG | Task | 처리 방식 |
|-----|------|----------|
| `make_risk_data` | `check_tb_livestock_species_information` | 데이터 조치 후 Success 또는 Clear |
| `make_risk_data` | `check_tb_prediction_result` | 데이터 확인 후 Success 처리 |

### 2.3 자동 Clear 가능 여부

- 단순 Clear로 80% 이상 해결되는 케이스는 **없음**
- 사육두수 이상감지의 경우 HIST 확인 후 큰 격차가 없으면 Success 처리하는 **반자동 판단**이 필요

---

## 3. 이상치(Anomaly) 판단 기준

### 3.1 판단 방식

단순 퍼센트 증감이 아닌 **종(Species) 특성을 반영한 판단**이 필요합니다.

### 3.2 판단 사례

| 종 | 사례 | 판단 | 근거 |
|----|------|------|------|
| 닭(가금류) | 6만수 → 1수 → 7만수 | **정상** | 기존 HIST에 6만수 이력이 존재하므로 이상 없음 |
| 한우 | 23마리 → 2,423마리 | **이상(오기입)** | 방역본부에서 오기입으로 판단, 데이터 수정 절차 진행 |

### 3.3 판단에 사용하는 데이터

```sql
-- 사육두수 히스토리 조회 (BRD_HAD_CO 컬럼이 사육두수)
SELECT * FROM TN_MOBILE_BLVSTCK_HIST
WHERE FRMHS_NO = '{농가번호}'
ORDER BY LAST_CHANGE_DT DESC;
```

- 근 1년 이내의 사육두수 이력을 기반으로 판단
- 에러 로그 예시: `00293965의 415006 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: 63000.0, 전일 사육두수: 62.0`

---

## 4. 수동 복구 절차 (현행 Workflow)

### 4.1 전체 흐름

```
에러 인지 (Airflow UI)
    │
    ▼
HIST 테이블 조회 (geoai DB)
    │
    ├─ 정상 판단 → Success 처리 ──────────────────────────► 완료
    │
    └─ 이상 판단 → DPL/LSFARM DB 교차 쿼리
                        │
                        ▼
                   농가번호 추적 (DPL → LSFARM)
                        │
                        ▼
                   방역본부 전화 (044-550-5581)
                   데이터 수정 요청
                        │
                        ▼
                   동기화 대기
                   LSFARM → DPL (실시간)
                         → M2M (+1일)
                         → geoai (+2일)
                        │
                        ▼
                   M2M 테이블 수동 수정
                   (TN_MOBILE_BLVSTCK_HIST / TN_MOBILE_BLVSTCK)
                        │
                        ▼
                   Airflow Task Clear 및 순차 재배치
                        │
                        ▼
                       완료
```

### 4.2 이상 데이터 추적 쿼리 (DPL → LSFARM)

```sql
-- Step 1: DPL DB에서 실제 농가번호 확인
SELECT * FROM TN_BLVSTCK
WHERE FRMHS_NO IN (
    SELECT a.FRMHS_NO 
    FROM VIEW_FRMHS a, TN_MOBILE_FRMHS_INFO@M2MSYS b
    WHERE a.FDB_FRMHS_SN = b.FDB_FRMHS_SN
      AND b.FRMHS_NO = '{에러농가번호}'
);

-- Step 2: LSFARM(방역본부) 농가번호 조회
SELECT B.CNTC_FRMHS_NO
FROM TN_FRMHS@DL_LSFARM A, TN_FARMHS_NO_MAPNG@DL_LSFARM B
WHERE A.FRMHS_SN = B.FRMHS_SN
  AND A.USE_AT = 'Y'
  AND B.USE_AT = 'Y'
  AND B.INSTT_SE_CODE = '03'
  AND A.FRMHS_SN = (
      SELECT FDB_FRMHS_SN FROM VIEW_FRMHS WHERE FRMHS_NO = '{에러농가번호}'
  );

-- Step 3: LSFARM DB에서 원천 데이터 확인
SELECT * FROM EAI_TN_FARM_INFO WHERE FARM_NO = '{LSFARM농가번호}';
SELECT * FROM EAI_TN_FARM_SCALE_DETAIL WHERE FARM_NO = '{LSFARM농가번호}' ORDER BY UPDT_DE DESC;
SELECT * FROM EAI_TN_FARM_SCALE WHERE FARM_NO = '{LSFARM농가번호}';
```

### 4.3 재배치 순서 (Task 의존관계)

```
make_risk_data
  └─ 차량_GPS_위험도
       └─ geoai_vehicle_risk_view_process
            └─ fulltime_risk_process
                 └─ region_risk_process
                      ├─ make_view_report_process_back → 방역권역_프로세스
                      └─ ASF_일일프로세스 → make_asf_view_process → 실시간_차량_프로세스
```

### 4.4 Airflow Retry로 해결 불가한 이유

1. **데이터 적합성 확인** 필요 (사람의 판단이 개입)
2. **원천 데이터 수정 프로세스** 대기 (방역본부 수정 → DB 동기화에 1~2일 소요)

---

## 5. 연동 시스템

| 시스템 | DB 종류 | 용도 | 비고 |
|--------|---------|------|------|
| geoai | PostgreSQL | 배치 대상 데이터, 위험도 산출 | 메인 운영 DB |
| m2msys | Oracle | 모바일 축산 데이터 중간 저장소 | LSFARM → DPL 이후 +1일 동기화 |
| dpl | Oracle | 데이터 플랫폼 | 실시간 동기화 |
| lsfarm | Oracle | 방역본부 원천 데이터 | 수정 요청 대상 |

---

## 6. 제약 사항 및 안전 규칙

### 6.1 절대 자동화 금지 사항

- **사육두수 데이터 자동 패스 금지**: 위험도 산출에 직접 영향을 미치므로, 검증 없이 잘못된 데이터가 삽입되면 안 됨
- 다중 Oracle DB(KAHIS, 방역본부 등) 접근이 필요한 확인 작업은 자동화 범위에서 제외 검토 필요

### 6.2 자동 재시도 제한

- 최대 재시도 횟수: **3회**

### 6.3 인프라 제약

- **폐쇄망** 환경에서 운영
- 여러 외부 Oracle DB와 통신해야 하므로, Kafka/Spring 컨테이너의 **방화벽 및 네트워크 통신 가능 여부** 사전 확인 필수

---

## 7. 희망 기능 (향후 고도화)

### 7.1 에러 문자 알림

- 배치 에러 발생 시 담당자에게 **SMS 알림** 발송
- 에러 유형, DAG/Task 명, 에러 메시지 요약 포함

### 7.2 원천 데이터 동기화 감지 (Polling)

- 방역본부에 수정 요청 후, Spring이 **하루 1회** 원천 DB를 조회
- 데이터 수정이 감지되면 자동으로 배치 재개
- 흐름: `수정 요청 → Polling 감지 → M2M 수정 → Task Clear → 재배치`

### 7.3 네트워크/방화벽 사전 점검

- Kafka 클러스터 및 Spring 컨테이너가 폐쇄망 내 각 Oracle DB와 통신 가능한지 아키텍처 수준에서 확인 필요
- 대상: m2msys, dpl, lsfarm 각 DB 서버

---

## 8. 시스템 설계 시 고려사항 요약

| 항목 | 결정 사항 |
|------|----------|
| 자동 Clear | 불가 (데이터 검증 선행 필수) |
| 반자동 지원 | HIST 조회 → 판단 근거 제시 → 운영자 승인 후 처리 |
| 재시도 횟수 | 최대 3회 |
| 이상치 판단 | 종 특성 반영 (단순 % 비교 불가) |
| 외부 DB 연동 | Oracle 3개 (m2msys, dpl, lsfarm) |
| 운영 환경 | 폐쇄망, 방화벽 확인 필요 |
| 알림 | SMS 문자 알림 필요 |
| 동기화 감지 | 원천 DB Polling 기능 희망 |