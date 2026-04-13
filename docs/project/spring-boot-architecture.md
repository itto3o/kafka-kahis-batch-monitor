# Spring Boot 애플리케이션 설계서

## 1. 전체 처리 흐름

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Phase 1: 에러 수신 및 토픽 발행                                               │
│                                                                              │
│  Airflow                    Spring Boot                     Kafka            │
│  (on_failure_callback)      (ErrorReceiveController)                         │
│                                                                              │
│  task_id +          ──►  1. task_id → 에러 유형 판별                           │
│  error_message(raw)      2. 에러 유형별 Parser로 metadata 추출                  │
│                          3. StatusLog 저장 (RECEIVED)                         │
│                          4. 에러 유형별 토픽 발행           ──►  에러 토픽 (5개)  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  Phase 2: 에러 유형별 검증 + 자동 조치 (Consumer 4개)                            │
│                                                                              │
│  error.livestock-anomaly  ──► LivestockAnomalyConsumer                        │
│                                    │                                         │
│                                    ▼                                         │
│                              LivestockVerificationService                    │
│                              (M2M HIST 조회 → 자동 판단)                      │
│                                    │                                         │
│                          ┌─────────┴──────────┐                              │
│                          ▼                     ▼                             │
│                       정상 판단              비정상 판단                         │
│                          │                     │                             │
│                          ▼                     ▼                             │
│                  AirflowClient          MANUAL_REVIEW_REQUIRED                │
│                  .clearTask()           (운영자가 Airflow UI에서                │
│                          │               직접 처리 — 이후 추적 없음)             │
│                          ▼                                                   │
│                  AUTO_CLEARED /                                              │
│                  AUTO_CLEAR_FAILED                                           │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────┐                │
│  │ 모든 Consumer는 처리 후 error-notification 토픽 발행 (SMS)  │                │
│  └──────────────────────────────────────────────────────────┘                │
│                                                                              │
│  error.prediction-anomaly ──► PredictionAnomalyConsumer                       │
│  error.asf-batch-failure  ──► AsfBatchFailureConsumer                         │
│  error.data-sync-failure  ──► DataSyncFailureConsumer                         │
│  (자동 검증 미지원 유형은 즉시 MANUAL_REVIEW_REQUIRED + 알림)                     │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  Phase 3: 알림 발송                                                           │
│                                                                              │
│  error-notification  ──► NotificationConsumer                                 │
│                                │                                             │
│                                ▼                                             │
│                          NotificationService                                 │
│                          (SMS 발송)                                           │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

> 운영자 승인 워크플로우는 본 시스템에 두지 않습니다. 운영자가 알림(SMS)을 받고 Airflow UI에서 직접 Clear/Mark Success 처리합니다.

---

## 1-1. 아키텍처 결정: Airflow → Spring → Kafka vs Airflow → Kafka 직접 발행

### Kafka가 의미 있는 구간

현재 구조에서 Kafka의 역할은 **Airflow → Spring 구간이 아니라, Spring 이후 구간**입니다.

```
Airflow → Spring (수신/파싱)  ← 여기는 단순 HTTP, Kafka 없어도 됨
                │
                ▼
            Kafka 토픽들      ← Kafka가 의미 있는 구간
                │
    ┌───────────┴───────────┐
    ▼                       ▼
검증/자동조치 Consumer      알림 Consumer
(HIST 분석 + Clear API)    (SMS 발송)
```

에러 수신 한 건이 errorType별 검증 Consumer로 라우팅되고, 검증 결과와 무관하게 알림 Consumer는 **동시에** 메시지를 받아 처리합니다. Consumer끼리 직접 호출하는 것이 아니라, **토픽을 통해 간접적으로 연결**됩니다 (Event-Driven Choreography 패턴).

이 프로젝트에서 Kafka가 실제로 주는 이점은 "병렬 분산"보다는:

1. **디커플링** — 검증 로직을 바꿔도 알림 로직에 영향 없음
2. **내구성** — Consumer가 죽어도 메시지가 토픽에 남아 있어 재처리 가능
3. **배압 처리** — 에러가 한꺼번에 몰려도 Consumer가 자기 속도로 처리

### 이 구조(API Gateway → Kafka 패턴)가 일반적인가?

네, 일반적인 패턴입니다.

- 외부 시스템(Airflow)은 HTTP만 알면 됨 — Kafka 프로토콜을 모르는 시스템과 연동할 때 표준적인 방법
- Spring이 **프로토콜 변환 + 라우팅** 역할을 함
- Airflow에서 Kafka 직접 호출하려면 `kafka-python` 의존성 + 브로커 주소 설정 + 직렬화 설정이 DAG 쪽에 필요

### 두 방식 비교

| 비교 | Airflow → Spring → Kafka | Airflow → Kafka 직접 |
|------|--------------------------|---------------------|
| Airflow 수정 범위 | callback 1개 (HTTP) | callback + kafka 설정 + 파싱 로직 |
| 파이프라인 팀 부담 | 낮음 | 높음 |
| 파싱/라우팅 변경 시 | Spring만 수정 | DAG callback 수정 |
| 방화벽 | Airflow → Spring 1개 | Airflow → Kafka 브로커 3개 |
| Kafka 의존성 | Airflow에 불필요 | kafka-python 필요 |

### 결론

파이프라인 팀 부담 최소화 + 폐쇄망 환경을 고려하면, **Spring을 거치는 현재 구조가 합리적**입니다.
Kafka의 가치는 수신 이후의 **비동기 파이프라인 구간**(디커플링, 내구성, 배압 처리)에서 나옵니다.

---

## 2. Consumer 역할 요약

### 2.1 에러 검증/자동 조치 Consumer (4개)

4개 Consumer 모두 다음 공통 흐름을 따릅니다.

1. `StatusLog` 상태 갱신 (`AUTO_VERIFYING`)
2. errorType별 검증 로직 수행 (자동 검증을 지원하는 경우)
3. 정상 판단 시 `AirflowClient.clearTask()` 호출 → `AUTO_CLEARED` / `AUTO_CLEAR_FAILED`
4. 비정상 또는 자동 검증 미지원 시 → `MANUAL_REVIEW_REQUIRED`
5. `error-notification` 토픽 발행 (운영자 인지용 SMS)

| Consumer | 소비 토픽 | 자동 검증 로직 | 자동 Clear |
|----------|----------|--------------|----------|
| `LivestockAnomalyConsumer` | `error.livestock-anomaly` | HIST 테이블 조회 → 근 1년 이력 기반 자동 판단 | ✓ |
| `PredictionAnomalyConsumer` | `error.prediction-anomaly` | (정책 결정 전 — 즉시 `MANUAL_REVIEW_REQUIRED`) | × |
| `AsfBatchFailureConsumer` | `error.asf-batch-failure` | 선행 배치 의존성으로 즉시 `MANUAL_REVIEW_REQUIRED` | × |
| `DataSyncFailureConsumer` | `error.data-sync-failure` | 원천 DB 불일치 — 즉시 `MANUAL_REVIEW_REQUIRED` | × |

### 2.2 공통 처리 Consumer (1개)

| Consumer | 소비 토픽 | 역할 |
|----------|----------|------|
| `NotificationConsumer` | `error-notification` | SMS 알림 발송 |

---

## 3. 패키지 구조

```
kr.go.kahis.batchmonitor/
│
├── Application.java
│
├── common/
│   └── config/
│       ├── PostgresDataSourceConfig.java        ← PostgreSQL + JPA 설정 (@Primary)
│       └── OracleDataSourceConfig.java          ← Oracle + MyBatis 설정
│
├── controller/
│   └── ErrorReceiveController.java              ← Airflow callback 수신 → 토픽 발행
│
├── messaging/                                   ← Kafka 이벤트 메시징
│   ├── consumer/
│   │   ├── LivestockAnomalyConsumer.java        ← error.livestock-anomaly 소비
│   │   ├── PredictionAnomalyConsumer.java       ← error.prediction-anomaly 소비
│   │   ├── AsfBatchFailureConsumer.java         ← error.asf-batch-failure 소비
│   │   ├── DataSyncFailureConsumer.java         ← error.data-sync-failure 소비
│   │   └── NotificationConsumer.java            ← error-notification 소비
│   ├── producer/
│   │   └── ErrorEventProducer.java              ← Kafka Producer (토픽 발행 공통)
│   └── dto/                                     ← Kafka 메시지 DTO
│       ├── ErrorEvent.java                      ← 에러 이벤트
│       └── NotificationEvent.java               ← 알림 이벤트
│
├── persistence/                                 ← 내부 DB 상태 관리 (PostgreSQL)
│   ├── entity/
│   │   └── StatusLog.java                       ← 에러 이벤트 상태 로그
│   └── repository/
│       └── StatusLogRepository.java
│
├── reader/                                      ← 외부 DB 읽기 전용 (Oracle)
│   └── mapper/
│       └── LivestockMapper.java
│
├── service/
│   ├── LivestockVerificationService.java        ← HIST 조회 + 자동 판단 + Clear 호출
│   ├── PredictionVerificationService.java       ← 예측치 검증
│   ├── AsfVerificationService.java              ← 선행 배치 상태 확인
│   ├── DataSyncVerificationService.java         ← 행 수 비교 검증
│   └── NotificationService.java                 ← SMS 발송
│
├── parser/
│   ├── ErrorMessageParser.java                  ← 파서 인터페이스
│   ├── LivestockErrorParser.java                ← 사육두수 에러 메시지 정규식 파싱
│   ├── PredictionErrorParser.java               ← 예측치 에러 메시지 정규식 파싱
│   └── DefaultErrorParser.java                  ← 기본 파서 (파싱 불가 시 원문 보존)
│
├── dto/
│   └── AirflowErrorRequest.java                 ← HTTP 요청 DTO
│
└── client/
    └── AirflowClient.java                       ← OpenFeign Airflow REST API 클라이언트

resources/
└── mapper/                                      ← MyBatis XML 매퍼
    └── LivestockMapper.xml
```

---

## 4. 주요 컴포넌트별 코드 설계

### 4.1 ErrorReceiveController (Airflow callback 수신)

Airflow `on_failure_callback`에서 HTTP POST로 전송한 에러를 수신합니다.
`task_id`로 에러 유형을 판별하고, 에러 메시지를 파싱하여 `StatusLog`에 `RECEIVED` 상태로 저장한 뒤 해당 토픽으로 발행합니다.

```java
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class ErrorReceiveController {

    private final ErrorEventProducer producer;
    private final StatusLogRepository statusLogRepository;
    private final Map<String, ErrorMessageParser> parsers;

    // task_id → 토픽 매핑
    private static final Map<String, String> TASK_TOPIC_MAP = Map.of(
        "check_tb_livestock_species_information", "error.livestock-anomaly",
        "check_tb_prediction_result",             "error.prediction-anomaly"
        // 매핑 없는 task_id는 "error.data-sync-failure"로 라우팅
    );

    @PostMapping("/errors")
    public ResponseEntity<?> receiveError(@RequestBody AirflowErrorRequest request) {

        // 1. task_id → 토픽 결정
        String topic = TASK_TOPIC_MAP.getOrDefault(
            request.getTaskId(), "error.data-sync-failure"
        );

        // 2. 에러 메시지 파싱 (실패 시 빈 metadata)
        ErrorMessageParser parser = parsers.getOrDefault(
            request.getTaskId(), new DefaultErrorParser()
        );
        Map<String, Object> metadata = parser.parse(request.getErrorMessage());

        // 3. StatusLog 저장 (RECEIVED)
        String eventId = UUID.randomUUID().toString();
        statusLogRepository.save(StatusLog.received(eventId, request, metadata));

        // 4. 이벤트 구성 + 토픽 발행
        ErrorEvent event = ErrorEvent.builder()
            .eventId(eventId)
            .dagId(request.getDagId())
            .taskId(request.getTaskId())
            .executionDate(request.getExecutionDate())
            .errorMessage(request.getErrorMessage())
            .tryNumber(request.getTryNumber())
            .metadata(metadata)
            .detectedAt(LocalDateTime.now())
            .build();

        producer.send(topic, event);
        return ResponseEntity.ok().build();
    }
}
```

### 4.2 ErrorEventProducer (Kafka 발행 공통)

```java
@Component
@RequiredArgsConstructor
public class ErrorEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void send(String topic, Object event) {
        kafkaTemplate.send(topic, event);
    }

    public void send(String topic, String key, Object event) {
        kafkaTemplate.send(topic, key, event);
    }
}
```

### 4.3 Consumer 예시 (사육두수)

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class LivestockAnomalyConsumer {

    private final LivestockVerificationService verificationService;
    private final AirflowClient airflowClient;
    private final ErrorEventProducer producer;
    private final StatusLogRepository statusLogRepository;

    @KafkaListener(topics = "error.livestock-anomaly", groupId = "kafka-consumer-group")
    public void consume(ErrorEvent event, Acknowledgment ack) {
        log.info("[사육두수 이상감지] 수신: farmNumber={}, speciesCode={}",
            event.getMetadata().get("farmNumber"),
            event.getMetadata().get("speciesCode"));

        StatusLog log = statusLogRepository.findByEventId(event.getEventId()).orElseThrow();

        // 1. 자동 검증 시작
        log.markAutoVerifying();

        // 2. HIST 조회 + 자동 판단
        VerificationResult result = verificationService.verify(event);
        log.applyVerification(result);

        // 3. 결과 분기
        if (result.isNormal()) {
            // 자동 처리: Airflow Clear API 호출
            try {
                ClearResponse resp = airflowClient.clearTask(
                    event.getDagId(), event.getTaskId(), event.getExecutionDate(), false);
                log.markAutoCleared(resp.statusCode());
            } catch (Exception e) {
                log.markAutoClearFailed(e.getMessage());
            }
        } else {
            // 운영자가 Airflow UI에서 직접 처리 — 이후 추적 없음
            log.markManualReviewRequired();
        }

        statusLogRepository.save(log);

        // 4. SMS 알림 발행 (운영자 인지용)
        producer.send("error-notification", event.getEventId(),
            NotificationEvent.from(event, log.getStatus()));

        // 5. 수동 ACK
        ack.acknowledge();
    }
}
```

### 4.4 LivestockVerificationService (HIST 조회 + 자동 판단)

사육두수 이상감지 시 M2M HIST 테이블을 조회하여 자동 판단 결과를 생성합니다. Clear API 호출은 Consumer에서 수행합니다.

```java
@Service
@RequiredArgsConstructor
public class LivestockVerificationService {

    private final JdbcTemplate jdbcTemplate;

    public VerificationResult verify(ErrorEvent event) {
        String farmNumber = (String) event.getMetadata().get("farmNumber");
        String speciesCode = (String) event.getMetadata().get("speciesCode");
        double currentValue = ((Number) event.getMetadata().get("currentValue")).doubleValue();

        // M2M HIST 조회 (근 1년)
        // geoai DB에 적재된 TN_MOBILE_BLVSTCK_HIST 테이블 조회
        List<Double> history = jdbcTemplate.queryForList(
            """
            SELECT brd_had_co as value
            FROM tn_mobile_blvstck_hist
            WHERE frmhs_no = ? AND lstksp_cl = ?
              AND change_dt >= CURRENT_DATE - INTERVAL '1 year'
            ORDER BY change_dt DESC
            """,
            Double.class,
            farmNumber, speciesCode
        );

        // 자동 판단: 근 1년 이력에서 현재값의 50%~200% 범위 내 수치가 있으면 LIKELY_NORMAL
        boolean normal = history.stream()
            .anyMatch(v -> v > currentValue * 0.5 && v < currentValue * 2.0);

        AutoJudgement judgement = normal ? AutoJudgement.LIKELY_NORMAL : AutoJudgement.LIKELY_ANOMALY;
        String reason = normal
            ? String.format("근 1년 내 %.0f수 범위 이력 존재", currentValue)
            : String.format("근 1년 내 %.0f수 범위 이력 없음 (오기입 의심)", currentValue);

        return new VerificationResult(judgement, reason);
    }
}
```

### 4.5 NotificationConsumer (SMS 발송)

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final NotificationService notificationService;

    @KafkaListener(topics = "error-notification", groupId = "kafka-consumer-group")
    public void consume(NotificationEvent event, Acknowledgment ack) {
        log.info("[알림 발송] type={}, title={}", event.getNotificationType(), event.getTitle());

        if ("SMS".equals(event.getNotificationType())) {
            notificationService.sendSms(event.getRecipients(), event.getTitle(), event.getMessage());
        }

        ack.acknowledge();
    }
}
```

---

## 5. Parser 설계

### 5.1 인터페이스

```java
public interface ErrorMessageParser {
    Map<String, Object> parse(String errorMessage);
}
```

### 5.2 사육두수 파서

```java
@Component("check_tb_livestock_species_information")
public class LivestockErrorParser implements ErrorMessageParser {

    // 00293965의 415006 사육두수 비교에 이상이 감지되었습니다. 당일 사육두수: 63000.0, 전일 사육두수: 62.0
    private static final Pattern PATTERN = Pattern.compile(
        "^(.+)의\\s+(\\S+)\\s+사육두수 비교에 이상이 감지되었습니다\\.\\s*당일 사육두수:\\s*([\\d.]+),\\s*전일 사육두수:\\s*([\\d.]+)$"
    );

    @Override
    public Map<String, Object> parse(String errorMessage) {
        Matcher m = PATTERN.matcher(errorMessage);
        if (m.matches()) {
            return Map.of(
                "farmNumber", m.group(1).trim(),
                "speciesCode", m.group(2).trim(),
                "currentValue", Double.parseDouble(m.group(3)),
                "previousValue", Double.parseDouble(m.group(4))
            );
        }
        return Map.of();  // 파싱 실패 시 빈 metadata
    }
}
```

### 5.3 예측치 파서

```java
@Component("check_tb_prediction_result")
public class PredictionErrorParser implements ErrorMessageParser {

    // 20456536의 예측값이 크게 차이납니다. 당일 예측치: 0.973676, 전일 예측치: 0.816708
    private static final Pattern PATTERN = Pattern.compile(
        "^(.+)의 예측값이 크게 차이납니다\\.\\s*당일 예측치:\\s*([\\d.]+),\\s*전일 예측치:\\s*([\\d.]+)$"
    );

    @Override
    public Map<String, Object> parse(String errorMessage) {
        Matcher m = PATTERN.matcher(errorMessage);
        if (m.matches()) {
            return Map.of(
                "farmSerialNo", m.group(1).trim(),
                "currentPrediction", Double.parseDouble(m.group(2)),
                "previousPrediction", Double.parseDouble(m.group(3))
            );
        }
        return Map.of();
    }
}
```

### 5.4 기본 파서 (파싱 불가 시)

```java
@Component
public class DefaultErrorParser implements ErrorMessageParser {

    @Override
    public Map<String, Object> parse(String errorMessage) {
        return Map.of();  // metadata 없이 error_message 원문만 보존
    }
}
```

---

## 6. 컴포넌트 간 데이터 흐름 상세

```
Airflow callback
    │
    │  POST /api/v1/errors
    │  { dag_id, task_id, error_message, try_number, ... }
    │
    ▼
ErrorReceiveController ─────────────────────────────────────────────────────────
    │                                                                           │
    │  task_id = "check_tb_livestock_species_information"                       │
    │  → topic = "error.livestock-anomaly"                                      │
    │  → parser = LivestockErrorParser                                          │
    │  → metadata = { farmNumber, speciesCode, currentValue, previousValue }    │
    │                                                                           │
    ├──► StatusLogRepository.save(StatusLog.received(...))   ← RECEIVED        │
    │                                                                           │
    ▼                                                                           │
ErrorEventProducer.send("error.livestock-anomaly", event)                       │
    │                                                                           │
    ▼                                                                           │
┌─ error.livestock-anomaly (Kafka 토픽) ────────────────────────────────────────┘
│
▼
LivestockAnomalyConsumer
    │
    ├──► StatusLog 조회 + markAutoVerifying()         ← AUTO_VERIFYING
    │
    ├──► LivestockVerificationService.verify(event)
    │       │
    │       │  TN_MOBILE_BLVSTCK_HIST 조회 (근 1년)
    │       │  → AutoJudgement: LIKELY_NORMAL / LIKELY_ANOMALY
    │       │
    │       ▼
    │    VerificationResult 반환
    │
    ├──► 결과 분기
    │       │
    │       ├── LIKELY_NORMAL ──► AirflowClient.clearTask()
    │       │                         │
    │       │                         ├── 성공 ──► AUTO_CLEARED          (종결)
    │       │                         └── 실패 ──► AUTO_CLEAR_FAILED     (종결)
    │       │
    │       └── LIKELY_ANOMALY ──► MANUAL_REVIEW_REQUIRED                 (종결)
    │                              (운영자가 Airflow UI에서 직접 처리,
    │                               이후 본 시스템 추적 없음)
    │
    └──► ErrorEventProducer.send("error-notification", noti)
            │
            ▼
         ┌─ error-notification (Kafka 토픽) ────────────────┐
         │                                                  │
         ▼                                                  │
       NotificationConsumer                                 │
            │                                               │
            ▼                                               │
       NotificationService.sendSms(...)                     │
            │                                               │
            ▼                                               │
       담당자 SMS 수신                                       │
                                                            │
         └──────────────────────────────────────────────────┘
```

---

## 7. 에러 이벤트 상태 추적

### 7.1 처리 원칙

본 시스템의 책임 범위는 다음과 같이 명확히 분리됩니다.

- **자동 처리 가능한 에러**: HIST 등을 분석해 정상으로 판단되면 Airflow Clear API를 직접 호출하고, 호출 결과를 로그 테이블에 기록합니다.
- **운영자 판단이 필요한 에러**: 자동 판단 불가 또는 자동 판단 결과가 비정상인 경우, 운영자가 **Airflow UI에서 직접** Clear/Mark Success 처리합니다. 이 경우 본 시스템은 "운영자 처리 필요" 상태까지만 기록하며, **이후 Airflow에서 어떤 조치가 이루어졌는지는 추적하지 않습니다.**

> 즉, 본 시스템은 "자동 조치가 가능한 케이스를 걸러서 자동화"하는 역할만 수행하며, 운영자 승인 워크플로우(승인 화면, 승인 내역 저장 등)는 가지지 않습니다.

### 7.2 상태 흐름

```
                          [에러 수신]
                              │
                              ▼
                          RECEIVED
                          (로그 저장)
                              │
                              ▼
                       errorType 라우팅
                              │
              ┌───────────────┴───────────────┐
              │                               │
              ▼                               ▼
       자동 검증 가능                    자동 검증 불가
       (LIVESTOCK_ANOMALY 등)           (DATA_SYNC_FAILURE 등)
              │                               │
              ▼                               │
          AUTO_VERIFYING                      │
          (HIST 조회/분석)                     │
              │                               │
       ┌──────┴──────┐                        │
       ▼             ▼                        │
    정상 판단     비정상 판단                    │
       │             │                        │
       ▼             ▼                        ▼
  Airflow      MANUAL_REVIEW_REQUIRED ◄───────┘
  Clear API    (운영자가 Airflow에서 직접 처리,
  호출          이후 추적 없음 — 종결)
       │
   ┌───┴───┐
   ▼       ▼
 성공    실패
   │       │
   ▼       ▼
AUTO_   AUTO_CLEAR_FAILED
CLEARED (운영자 수동 개입 필요 — 종결)
(종결)
```

모든 종결 상태(`AUTO_CLEARED`, `AUTO_CLEAR_FAILED`, `MANUAL_REVIEW_REQUIRED`)에 도달하면 본 시스템 내에서 더 이상의 상태 전이는 발생하지 않습니다.

### 7.3 상태 정의

| 상태 | 시점 | 설정 주체 | 종결 여부 | 설명 |
|------|------|----------|----------|------|
| `RECEIVED` | 에러 수신 직후 | `ErrorReceiveController` | × | 로그 테이블 저장 직후 초기 상태 |
| `AUTO_VERIFYING` | 자동 검증 시작 | Verification Consumer | × | HIST 조회 등 자동 분석 진행 중 |
| `AUTO_CLEARED` | Airflow Clear API 성공 | Verification Consumer | ✓ | 자동 정상 판단 후 Clear 실행 완료 |
| `AUTO_CLEAR_FAILED` | Airflow Clear API 실패 | Verification Consumer | ✓ | Clear 호출 자체가 실패 — 운영자 수동 개입 필요 |
| `MANUAL_REVIEW_REQUIRED` | 자동 판단 불가 / 비정상 | Verification Consumer 또는 `ErrorReceiveController` | ✓ | 운영자가 Airflow에서 직접 처리 — **이후 추적 없음** |

### 7.4 errorType별 처리 정책

| errorType | 자동 처리 가능? | 동작 |
|-----------|---------------|------|
| `LIVESTOCK_ANOMALY` | ✓ | HIST 조회 → 정상 시 Clear, 비정상 시 `MANUAL_REVIEW_REQUIRED` |
| `PREDICTION_ANOMALY` | △ (정책 결정 필요) | 기본은 `MANUAL_REVIEW_REQUIRED`, 향후 자동 판단 로직 추가 가능 |
| `ASF_BATCH_FAILURE` | × | 즉시 `MANUAL_REVIEW_REQUIRED` (선행 배치 의존성 때문) |
| `DATA_SYNC_FAILURE` | × | 즉시 `MANUAL_REVIEW_REQUIRED` (원천 DB 불일치는 사람 확인 필요) |
| `HPAI_DISPLAY_MISSING` | × | 즉시 `MANUAL_REVIEW_REQUIRED` (수동 등록 유형) |

> 자동 처리 정책은 errorType 단위로 enum/설정으로 관리해, 운영 안정화에 따라 단계적으로 자동화 범위를 넓힐 수 있도록 합니다.

### 7.5 StatusLog 엔티티

```java
@Entity
@Table(
    schema = "batch_monitor",
    name = "STATUS_LOG",
    indexes = {
        @Index(name = "IDX_STATUS_LOG_CREATE_AT", columnList = "create_at"),
        @Index(name = "IDX_STATUS_LOG_EVENT_ID", columnList = "event_id", unique = true),
        @Index(name = "IDX_STATUS_LOG_STATUS", columnList = "status")
    }
)
@Getter
@DynamicInsert
@DynamicUpdate
@NoArgsConstructor(access = PROTECTED)
@Comment("배치 에러 이벤트 상태 로그")
public class StatusLog {

    @Id
    @TsuId
    private String id;

    // Kafka 이벤트 식별자 (도메인 키)
    @Column(nullable = false, unique = true)
    private String eventId;

    // Airflow 발생 정보
    private String dagId;
    private String taskId;
    private String executionDate;
    private Integer tryNumber;

    @Enumerated(EnumType.STRING)
    private ErrorType errorType;        // LIVESTOCK_ANOMALY / PREDICTION_ANOMALY / ASF_BATCH_FAILURE / DATA_SYNC_FAILURE / HPAI_DISPLAY_MISSING / UNKNOWN

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @Column(columnDefinition = "TEXT")
    private String metadataJson;        // Parser 추출 metadata (JSON)

    // 자동 검증 결과 (자동 검증을 수행한 경우만)
    @Enumerated(EnumType.STRING)
    private AutoJudgement autoJudgement;    // LIKELY_NORMAL / LIKELY_ANOMALY / UNKNOWN

    @Column(columnDefinition = "TEXT")
    private String judgementReason;

    // 상태 추적
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;              // RECEIVED / AUTO_VERIFYING / AUTO_CLEARED / AUTO_CLEAR_FAILED / MANUAL_REVIEW_REQUIRED

    // Airflow Clear API 호출 결과 (자동 처리 케이스만)
    private LocalDateTime clearRequestedAt;     // Clear API 호출 시각
    private Integer clearHttpStatus;             // Airflow API 응답 상태 코드

    @Column(columnDefinition = "TEXT")
    private String clearFailureReason;           // AUTO_CLEAR_FAILED 사유

    // 타임스탬프
    private LocalDateTime detectedAt;            // Airflow 에러 감지 시각
    private LocalDateTime statusUpdatedAt;       // 마지막 상태 전이 시각
    private LocalDateTime createAt;              // 레코드 생성 시각
}
```

> **연쇄 에러 추적 제거**: 운영자가 Airflow에서 직접 Clear한 이후의 처리는 본 시스템이 알 수 없으므로, 이전 설계에 있던 `parentEventId` / `DOWNSTREAM_FAILED` 개념은 더 이상 유지하지 않습니다. 새 에러는 항상 독립된 이벤트로 기록됩니다.

### 7.6 ErrorReceiveController 처리 흐름

에러 수신 시점에는 errorType만 판별하여 `RECEIVED` 상태로 저장하고, 토픽 발행 후 종료합니다. 자동 검증 가능 여부에 따른 분기는 Consumer 단계에서 수행합니다.

```java
@PostMapping("/errors")
public ResponseEntity<?> receiveError(@RequestBody AirflowErrorRequest request) {

    // 1. errorType 판별 + 토픽 결정
    String topic = TASK_TOPIC_MAP.getOrDefault(request.getTaskId(), "error.data-sync-failure");

    // 2. 에러 메시지 파싱
    Map<String, Object> metadata = parsers.getOrDefault(
        request.getTaskId(), new DefaultErrorParser()
    ).parse(request.getErrorMessage());

    // 3. StatusLog 저장 (RECEIVED)
    String eventId = UUID.randomUUID().toString();
    statusLogRepository.save(StatusLog.received(eventId, request, metadata));

    // 4. 토픽 발행
    ErrorEvent event = ErrorEvent.builder()
        .eventId(eventId)
        .dagId(request.getDagId())
        .taskId(request.getTaskId())
        .executionDate(request.getExecutionDate())
        .errorMessage(request.getErrorMessage())
        .tryNumber(request.getTryNumber())
        .metadata(metadata)
        .detectedAt(LocalDateTime.now())
        .build();

    producer.send(topic, event);
    return ResponseEntity.ok().build();
}
```

### 7.7 Verification Consumer 처리 흐름 (사육두수 예시)

```java
@KafkaListener(topics = "error.livestock-anomaly", groupId = "kafka-consumer-group")
public void consume(ErrorEvent event, Acknowledgment ack) {

    StatusLog log = statusLogRepository.findByEventId(event.getEventId()).orElseThrow();

    // 1. 자동 검증 시작
    log.markAutoVerifying();

    // 2. HIST 조회 + 자동 판단
    VerificationResult result = verificationService.verify(event);
    log.applyVerification(result);  // autoJudgement, judgementReason 기록

    // 3. 결과에 따른 분기
    if (result.isNormal()) {
        // 자동 처리: Airflow Clear API 호출
        try {
            ClearResponse resp = airflowClient.clearTask(
                event.getDagId(), event.getTaskId(), event.getExecutionDate(), false);
            log.markAutoCleared(resp.statusCode());
        } catch (Exception e) {
            log.markAutoClearFailed(e.getMessage());
        }
    } else {
        // 운영자 수동 처리 대상 — Airflow에서 직접 처리, 이후 추적 없음
        log.markManualReviewRequired();
    }

    statusLogRepository.save(log);

    // 알림 발송 (운영자 인지용)
    producer.send("error-notification", event.getEventId(), NotificationEvent.from(event));

    ack.acknowledge();
}
```

자동 검증을 수행하지 않는 errorType의 Consumer는 위 흐름에서 검증/Clear 단계를 생략하고 곧바로 `MANUAL_REVIEW_REQUIRED`로 전이합니다.
