# Spring Boot 애플리케이션 설계서

## 1. 전체 처리 흐름

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Phase 1: 에러 수신 및 토픽 발행                                                │
│                                                                              │
│  Airflow                    Spring Boot                     Kafka             │
│  (on_failure_callback)      (StatusLogController                              │
│                              + StatusLogServiceImpl)                          │
│                                                                              │
│  POST /api/v1/errors ──►  1. ParserUtil로 errorType + metadata 추출            │
│  (JSON)                   2. StatusLogUnit.create(StatusLog statusType=RECEIVED)│
│                           3. KafkaEventProducer.publish() →   ──►  에러 토픽 (9개) │
│                              ErrorType.topic 으로 라우팅                       │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  Phase 2: 에러 유형별 검증 + 자동 조치 (Consumer 2개)                            │
│                                                                              │
│  error.livestock-anomaly                                                      │
│      ──► KafkaLivestockErrorEventConsumer                                     │
│              │                                                                │
│              ▼                                                                │
│         StatusLog AUTO_VERIFYING 이력 추가                                     │
│              │                                                                │
│              ▼                                                                │
│         KahisServiceImpl.analysis()                                          │
│              │                                                                │
│              ├──► LivestockHistoryAnalyzer.analyze()                          │
│              │       (FarmMapper로 HIST 조회 → AnalyzeResultData 반환)         │
│              │                                                                │
│              ├──► LsFarmIdFinder.find()                                       │
│              │       (DPL → LSFARM 농가번호 매핑)                              │
│              │                                                                │
│              └──► 결과 분기                                                    │
│                    ├── LIKELY_NORMAL  → AUTO_CLEARED 이력 추가                 │
│                    │                    (※ Airflow Clear API 호출은 TODO)      │
│                    └── 그 외           → MANUAL_REVIEW_REQUIRED 이력 추가       │
│                                                                              │
│  분석 미지원 토픽 (notAnalysisTopics 자동 산출)                                │
│      ──► KafkaEventConsumer                                                   │
│              └─► 즉시 MANUAL_REVIEW_REQUIRED + JudgementType.UNKNOWN          │
│                  reason="운영자 검증이 필요한 에러 유형입니다."                   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

> Phase 3 (SMS 알림) 및 Airflow Clear API 호출은 미구현. TODO로 표시됨.
```

> 운영자 승인 워크플로우는 본 시스템에 두지 않습니다. 운영자가 알림(향후 SMS)을 받고 Airflow UI에서 직접 Clear/Mark Success 처리합니다.

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

### 2.1 에러 검증/자동 조치 Consumer (현재 2개 구현)

**상태는 갱신이 아니라 새 이력 row를 추가하는 방식(append-only)**으로 기록합니다.

`KafkaLivestockErrorEventConsumer` 흐름 (자동 검증 가능 유형):

1. `StatusLog` 이력 추가 (`AUTO_VERIFYING`, `lsfarmId=null`)
2. `KahisServiceImpl.analysis()` 호출 — try/catch로 감싸고 finally에서 ack
   - `LivestockHistoryAnalyzer.analyze()` — HIST 조회 + tolerance 비교 → `AnalyzeResultData(judgementType, reason)` 반환
   - `LsFarmIdFinder.find()` — DPL→LSFARM 농가번호 매핑
   - 결과 분기:
     - `LIKELY_NORMAL` → `AUTO_CLEARED` 이력 추가 (※ Airflow Clear API 호출은 TODO)
     - 그 외 (`LIKELY_ANOMALY` / `UNKNOWN`) → `MANUAL_REVIEW_REQUIRED` 이력 추가
3. 예외 발생 시 `log.error` 후 ack (※ MANUAL_REVIEW_REQUIRED 종결 row 추가는 미구현)

`KafkaEventConsumer` 흐름 (분석 미지원 유형 — `ErrorType.notAnalysisTopics()`로 자동 라우팅):

1. 즉시 `MANUAL_REVIEW_REQUIRED` 이력 추가 + `JudgementType.UNKNOWN`
2. ack

| Consumer | 소비 토픽 | 자동 검증 로직 | 자동 Clear |
|----------|----------|--------------|----------|
| `KafkaLivestockErrorEventConsumer` | `error.livestock-anomaly` | HIST 조회 + tolerance ×0.5~×2.0 매칭 | △ (이력만 기록, API 호출 TODO) |
| `KafkaEventConsumer` | `ErrorType.isNeedAnalysis = false` 인 모든 토픽 (예: `error.prediction-anomaly`, `error.asf-batch-failure` 등) | 없음 — 즉시 `MANUAL_REVIEW_REQUIRED` | × |

### 2.2 공통 처리 Consumer (미구현)

| Consumer | 소비 토픽 | 역할 |
|----------|----------|------|
| `NotificationConsumer` (TODO) | `error-notification` | SMS 알림 발송 |

---

## 3. 패키지 구조 (현재 구현 기준)

```
kr.go.kahis.batchmonitor/
│
├── Application.java                                 ← @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
│
├── common/
│   ├── annotation/
│   │   ├── TsuId.java                               ← ID 생성 어노테이션
│   │   └── Unit.java                                ← 영속 단위 컴포넌트 마커
│   ├── config/
│   │   ├── PostgresDataSourceConfig.java            ← PostgreSQL + JPA 설정 (@Primary, @EnableJpaAuditing)
│   │   ├── OracleDataSourceConfig.java              ← Oracle + MyBatis 설정
│   │   ├── OpenFeignConfig.java                     ← @EnableFeignClients + Feign 공통 설정
│   │   └── AirflowClientConfig.java                 ← AirflowClient용 BasicAuth interceptor
│   ├── enumeration/
│   │   └── ErrorType.java                           ← 에러 유형 + topic + isNeedAnalysis
│   ├── extension/
│   │   └── UnitDefaultExtension.java
│   └── generator/
│       └── TsuIdGenerator.java
│
├── controller/
│   └── StatusLogController.java                     ← Airflow callback 수신 (POST /api/v1/errors)
│
├── dto/
│   ├── data/
│   │   └── AnalyzeResultData.java                   ← (judgementType, reason) record
│   └── request/
│       └── ErrorRequest.java                        ← Airflow callback 요청 DTO
│
├── domain/
│   ├── airflow/                                     ← Airflow REST 호출 (Feign)
│   │   ├── client/
│   │   │   └── AirflowClient.java                   ← @FeignClient (※ 호출부는 미구현)
│   │   └── dto/
│   │       ├── request/TaskClearRequest.java
│   │       └── response/TaskClearResponse.java
│   │
│   ├── kafka/
│   │   ├── consumer/
│   │   │   ├── KafkaConsumer.java                   ← 마커 인터페이스
│   │   │   ├── KafkaEventConsumer.java              ← 분석 미지원 토픽 묶음 처리
│   │   │   └── KafkaLivestockErrorEventConsumer.java← 사육두수 토픽 전담
│   │   ├── dto/
│   │   │   └── KafkaEvent.java                      ← Kafka 메시지 record
│   │   └── producer/
│   │       └── KafkaEventProducer.java
│   │
│   ├── kahis/                                       ← 외부 DB 읽기 (Oracle, MyBatis)
│   │   ├── dto/
│   │   │   ├── MobileBreedingLivestockHistoryDto.java
│   │   │   ├── FarmIdDplDto.java
│   │   │   ├── FarmIdLsfarmDto.java
│   │   │   ├── FarmInfoDto.java
│   │   │   ├── FarmScaleDto.java
│   │   │   └── FarmScaleDetailDto.java
│   │   └── mapper/
│   │       └── FarmMapper.java
│   │
│   ├── parser/                                      ← 에러 메시지 정규식 파싱
│   │   ├── ParserUtil.java                          ← 정적 파서 (정규식 기반 라우팅)
│   │   ├── ParsedError.java
│   │   ├── PatternParser.java
│   │   └── data/                                    ← 에러 유형별 metadata DTO
│   │       ├── LivestockErrorData.java
│   │       ├── PredictionErrorData.java
│   │       ├── PnuErrorData.java
│   │       ├── CoordinateErrorData.java
│   │       ├── NotFoundErrorData.java
│   │       └── UnknownErrorData.java
│   │
│   └── statuslog/                                   ← 내부 DB (PostgreSQL)
│       ├── entity/
│       │   └── StatusLog.java                       ← append-only 이력 엔티티 (lsfarmId 포함)
│       ├── enumeration/
│       │   ├── StatusType.java                      ← RECEIVED / AUTO_VERIFYING / AUTO_CLEARED / AUTO_CLEAR_SUCCESS / AUTO_CLEAR_FAILED / MANUAL_REVIEW_REQUIRED
│       │   └── JudgementType.java                   ← LIKELY_NORMAL / LIKELY_ANOMALY / UNKNOWN
│       ├── repository/
│       │   └── StatusLogRepository.java
│       └── unit/
│           ├── StatusLogUnit.java                   ← 영속 단위 인터페이스
│           └── StatusLogUnitImpl.java               ← @Unit + @Transactional
│
├── service/
│   ├── StatusLogService.java / StatusLogServiceImpl.java       ← 수신/파싱/저장/Kafka 발행
│   ├── KahisService.java    / KahisServiceImpl.java          ← 사육두수 분석 오케스트레이션
│
└── vo/
    ├── LivestockHistoryAnalyzer.java                ← FarmMapper 호출 + tolerance 분석
    └── LsFarmIdFinder.java                          ← DPL → LSFARM 농가번호 변환

resources/
├── application.yml / application-local.yml / application-prod.yml
└── mapper/
    └── FarmMapper.xml                               ← MyBatis XML
```

> `AirflowClient` (Feign 클라이언트)는 정의되어 있지만 실제 호출 코드는 미연결입니다. `NotificationConsumer`, `error-notification` 토픽은 아직 구현되지 않았습니다.

---

## 4. 주요 컴포넌트별 코드 (현재 구현)

### 4.1 StatusLogController (Airflow callback 수신)

Airflow `on_failure_callback`에서 JSON POST로 전송한 에러를 수신합니다.
`@RequestBody`로 `ErrorRequest` 바인딩 후 `StatusLogServiceImpl.publish()`에 위임하고 `202 Accepted`로 응답합니다.

```java
@RestController
@RequiredArgsConstructor
public class StatusLogController {

  private final StatusLogService service;

  @PostMapping("/api/v1/errors")
  public ResponseEntity<?> publishError(@RequestBody ErrorRequest request) {
    service.publish(request);
    return ResponseEntity.accepted().build();
  }
}
```

`ErrorRequest`는 `@JsonProperty` 기반 record (snake_case JSON 키 → camelCase Java 필드 매핑). `executionDate`는 `LocalDate` + `@JsonFormat(pattern = "yyyy-MM-dd")`로 받아 callback의 `context["ds"]` 값과 매칭됩니다.

### 4.2 StatusLogServiceImpl (파싱 + 저장 + 토픽 발행)

```java
@Service
@RequiredArgsConstructor
public class StatusLogServiceImpl implements StatusLogService {

  private final StatusLogUnit statusLogUnit;
  private final KafkaEventProducer producer;

  @Override
  @Transactional
  public void publish(ErrorRequest dto) {
    ParsedError parsed = ParserUtil.parse(dto.errorMessage());
    int nowNano = LocalDateTime.now().getNano();
    String eventId = dto.taskId() + "-" + nowNano;

    statusLogUnit.create(StatusLog.builder()
        .eventId(eventId)
        .dagId(dto.dagId())
        .taskId(dto.taskId())
        .errorType(parsed.errorType())
        .errorMessage(dto.errorMessage())
        .metadata(parsed.metadata().toString())
        .statusType(StatusType.RECEIVED)
        .build());

    producer.publish(eventId, dto.dagId(), dto.taskId(), parsed.errorType(),
        dto.errorMessage(), parsed.metadata());
  }
}
```

> `eventId`는 현재 `taskId + "-" + LocalDateTime.now().getNano()` 형식으로 생성. 동일 nano 윈도우에서 충돌 가능성이 있어 결정적 키 또는 UUID로 변경하는 것이 향후 개선 후보.

### 4.3 KafkaEventProducer

`ErrorType.topic`으로 라우팅하고 key는 `dagId-taskId-yyyyMMdd` 조합. 콜백으로 발행 결과를 로깅합니다.

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventProducer {

  private final KafkaTemplate<String, KafkaEvent> kafkaTemplate;

  public void publish(String eventId, String dagId, String taskId, ErrorType errorType,
      String errorMessage, Map<String, String> metadata) {
    String topic = errorType.getTopic();
    LocalDateTime now = LocalDateTime.now();
    String key = dagId + "-" + taskId + "-" + now.toLocalDate().toString();
    KafkaEvent event = new KafkaEvent(eventId, dagId, taskId, errorType, errorMessage,
        metadata, now);

    kafkaTemplate.send(topic, key, event)
        .whenComplete((result, throwable) -> {
          if (throwable != null) {
            log.error("Kafka publish error: topic={}, eventId={}", topic, eventId, throwable);
            return;
          }
          log.info("Kafka publish success: topic={}, partition={}, offset={}, eventId={}", topic,
              result.getRecordMetadata().partition(), result.getRecordMetadata().offset(), eventId);
        });
  }
}
```

### 4.4 KafkaLivestockErrorEventConsumer (사육두수 전담)

`AUTO_VERIFYING` 이력을 먼저 추가하고 분석을 호출합니다. 분석 중 예외 발생 시 로그만 남기고 ack를 보냅니다.

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaLivestockErrorEventConsumer implements KafkaConsumer {

  private final StatusLogUnit statusLogUnit;
  private final KahisService kahisService;

  @KafkaListener(
      topics = "#{T(kr.go.kahis.batchmonitor.common.enumeration.ErrorType).LIVESTOCK_ANOMALY.topic}",
      groupId = "${spring.kafka.consumer.group-id}"
  )
  public void consume(KafkaEvent event, Acknowledgment acknowledgment) {
    statusLogUnit.create(StatusLog.builder()
        .eventId(event.eventId())
        .dagId(event.dagId())
        .taskId(event.taskId())
        .lsfarmId(null)
        .errorType(event.errorType())
        .errorMessage(event.errorMessage())
        .metadata(event.metadata().toString())
        .statusType(StatusType.AUTO_VERIFYING)
        .judgementType(null)
        .reason(null)
        .build());

    try {
      kahisService.analysis(event,
          event.metadata().get("farmNumber"),
          event.metadata().get("speciesCode"),
          Long.parseLong(event.metadata().get("currentValue")));
    } catch (Exception e) {
      log.error("Unexpected error during livestock analysis: {}", event.eventId(), e);
    } finally {
      acknowledgment.acknowledge();
    }
  }
}
```

> 예외 발생 시 추가로 `MANUAL_REVIEW_REQUIRED` 종결 row를 남기는 처리는 미구현 — 현재는 `AUTO_VERIFYING` row만 남고 끝납니다.

### 4.5 KafkaEventConsumer (분석 미지원 토픽 묶음)

`ErrorType.notAnalysisTopics()`가 자동으로 산출하는 토픽들을 한 Consumer가 모두 구독합니다.

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventConsumer implements KafkaConsumer {

  private final StatusLogUnit statusLogUnit;

  @KafkaListener(
      topics = "#{T(kr.go.kahis.batchmonitor.common.enumeration.ErrorType).notAnalysisTopics()}",
      groupId = "${spring.kafka.consumer.group-id}"
  )
  public void consume(KafkaEvent event, Acknowledgment acknowledgment) {
    statusLogUnit.create(StatusLog.builder()
        .eventId(event.eventId())
        .dagId(event.dagId())
        .taskId(event.taskId())
        .lsfarmId(null)
        .errorType(event.errorType())
        .errorMessage(event.errorMessage())
        .metadata(event.metadata().toString())
        .statusType(StatusType.MANUAL_REVIEW_REQUIRED)
        .judgementType(JudgementType.UNKNOWN)
        .reason("운영자 검증이 필요한 에러 유형입니다.")
        .build());

    log.info("receive not need analysis error: eventId={}, type={}", event.eventId(),
        event.errorType());

    acknowledgment.acknowledge();
  }
}
```

### 4.6 KahisServiceImpl (분석 오케스트레이션)

```java
@Service
@RequiredArgsConstructor
public class KahisServiceImpl implements KahisService {

  private final LivestockHistoryAnalyzer analyzer;
  private final LsFarmIdFinder finder;
  private final StatusLogUnit statusLogUnit;

  @Override
  public void analysis(KafkaEvent event, String farmNumber, String speciesCode, long currentCount) {
    AnalyzeResultData analyzeResult = analyzer.analyze(farmNumber, speciesCode, currentCount);
    String lsFarmId = finder.find(farmNumber);

    StatusType statusType = analyzeResult.judgementType() == JudgementType.LIKELY_NORMAL
        ? StatusType.AUTO_CLEARED
        : StatusType.MANUAL_REVIEW_REQUIRED;

    statusLogUnit.create(StatusLog.builder()
        .eventId(event.eventId())
        .dagId(event.dagId())
        .taskId(event.taskId())
        .lsfarmId(lsFarmId)
        .errorType(event.errorType())
        .errorMessage(event.errorMessage())
        .metadata(event.metadata().toString())
        .statusType(statusType)
        .judgementType(analyzeResult.judgementType())
        .reason(analyzeResult.reason())
        .build());

    // TODO: 정상 판단 시 Airflow Clear API 호출 (운영 환경에서 신뢰성 확보 후)
  }
}
```

> 위 코드는 흐름을 보여주기 위해 if/else 두 빌더 호출을 statusType 변수로 합쳐 단순화한 형태입니다. 실제 코드는 if/else 블록 두 벌로 작성되어 있습니다. `lsFarmId`는 finder가 NPE를 던질 가능성이 있어 분석 흐름 전체를 막을 수 있습니다 — 향후 null 허용 흐름으로 보완 필요.

### 4.7 LivestockHistoryAnalyzer (HIST tolerance 분석)

근 12개월 HIST를 조회하고 `currentCount × [0.5, 2.0]` 범위 내 매칭값 존재 여부로 판단합니다.

```java
@Component
@RequiredArgsConstructor
public class LivestockHistoryAnalyzer {

  private final FarmMapper mapper;

  public AnalyzeResultData analyze(String farmId, String speciesCode, long currentCount) {
    List<MobileBreedingLivestockHistoryDto> history =
        mapper.selectMobileBreedingLivestockHistory(farmId, speciesCode);

    if (history.isEmpty()) {
      return new AnalyzeResultData(JudgementType.UNKNOWN, "HIST 부재");
    }

    List<Long> counts = history.stream()
        .filter(hist -> hist.brdHadCo() != null && hist.lastChangeDt() != null)
        .sorted(Comparator.comparing(MobileBreedingLivestockHistoryDto::lastChangeDt).reversed())
        .map(MobileBreedingLivestockHistoryDto::brdHadCo)
        .toList();

    double low  = currentCount * 0.5;
    double high = currentCount * 2.0;

    List<Long> matched = counts.stream()
        .filter(count -> count >= low && count <= high)
        .toList();

    if (!matched.isEmpty()) {
      long mostRecent = matched.get(0);              // 정렬 + filter 순서 보존으로 보장
      long closest = matched.stream()
          .min(Comparator.comparingLong(c -> Math.abs(c - currentCount)))
          .orElseThrow();
      return new AnalyzeResultData(
          JudgementType.LIKELY_NORMAL,
          "HIST 정상 — 당일값 " + currentCount
              + ", 허용범위(×0.5 ~ ×2.0) 내 매칭값 존재 (최근= " + mostRecent
              + ", 최근접= " + closest + ")");
    }

    return new AnalyzeResultData(
        JudgementType.LIKELY_ANOMALY,
        "HIST 비정상 — 당일값 " + currentCount
            + ", 허용범위(×0.5 ~ ×2.0) 내 매칭값 미존재");
  }
}
```

> `currentCount = 0`일 때 동작은 REQUIREMENTS.md 3.5.6 참조. 현재는 별도 분기 없이 HIST에 0이 있으면 자동 Clear 흐름을 탑니다.

### 4.8 LsFarmIdFinder (DPL → LSFARM 농가번호 매핑)

```java
@Component
@RequiredArgsConstructor
public class LsFarmIdFinder {

  private final FarmMapper mapper;

  public String find(String farmId) {
    String dplFarmId = mapper.selectFarmIdDpl(farmId).frmhsNo();
    return mapper.selectFarmIdLsfarm(dplFarmId).cntcFrmhsNo();
  }
}
```

> 매퍼 결과가 null이면 `.frmhsNo()` / `.cntcFrmhsNo()`에서 NPE 발생. lsfarmId는 부가 정보이므로 null 허용 흐름으로 향후 보완 후보.

### 4.9 NotificationConsumer (TODO)

`error-notification` 토픽 + SMS 발송 Consumer는 아직 구현되지 않았습니다.

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

## 6. 컴포넌트 간 데이터 흐름 상세 (현재 구현)

```
Airflow callback
    │
    │  POST /api/v1/errors  (JSON)
    │  dag_id, task_id, execution_date, error_message, try_number
    │
    ▼
StatusLogController ───────────────────────────────────────────────────────────
    │                                                                          │
    │  @RequestBody ErrorRequest                                               │
    │                                                                          │
    ▼                                                                          │
StatusLogServiceImpl.publish()                                                 │
    │                                                                          │
    │  1. ParserUtil.parse(errorMessage) → ParsedError(errorType, metadata)    │
    │     · 정규식 라우팅: LIVESTOCK_ANOMALY / PREDICTION_ANOMALY /             │
    │                     PNU_ANOMALY / FARM_COORDINATE_MISSING / ...          │
    │     · 매칭 실패 시 errorType = UNKNOWN, metadata = {}                     │
    │                                                                          │
    │  2. eventId = taskId + LocalDateTime.now().getNano()                     │
    │                                                                          │
    │  3. statusLogUnit.create(statusType=RECEIVED, lsfarmId=null)             │
    │                                                                          │
    │  4. KafkaEventProducer.publish(...) → ErrorType.topic 으로 라우팅         │
    │                                                                          │
    ▼                                                                          │
202 Accepted ──────────────────────────────────────────────────────────────────┘

       │
       ▼  (Kafka 토픽들)
┌──────────────────────────────────────┐    ┌────────────────────────────────┐
│ error.livestock-anomaly              │    │ error.* (그 외, isNeedAnalysis │
│  ──► KafkaLivestockErrorEventConsumer│    │      = false 인 모든 토픽)      │
│                                      │    │  ──► KafkaEventConsumer        │
└──────────────────────────────────────┘    └────────────────────────────────┘
       │                                              │
       ▼                                              ▼
1. AUTO_VERIFYING 이력 추가                    MANUAL_REVIEW_REQUIRED 이력
   (lsfarmId=null)                            + JudgementType.UNKNOWN
       │                                       + reason="운영자 검증이 필요한
       ▼                                          에러 유형입니다."
2. KahisServiceImpl.analysis()                       │
   try { ... } catch (Exception e) { log }            ▼
   finally { ack }                                ack (종결)
       │
       ├──► LivestockHistoryAnalyzer.analyze()
       │       FarmMapper.selectMobileBreedingLivestockHistory(farmId, speciesCode)
       │       → counts (12개월 + null 제거 + lastChangeDt DESC)
       │       → tolerance ×0.5~×2.0 매칭
       │       → AnalyzeResultData(judgementType, reason)
       │
       ├──► LsFarmIdFinder.find(farmNumber)
       │       FarmMapper.selectFarmIdDpl → DPL FRMHS_NO
       │       FarmMapper.selectFarmIdLsfarm → LSFARM CNTC_FRMHS_NO
       │
       └──► statusLogUnit.create(...)
              ├── LIKELY_NORMAL → AUTO_CLEARED 이력 추가
              │   (※ Airflow Clear API 호출은 TODO)
              └── 그 외        → MANUAL_REVIEW_REQUIRED 이력 추가
              그리고 ack
```

> SMS 알림(`error-notification` 토픽 + NotificationConsumer)은 아직 구현되지 않았습니다.

---

## 7. 에러 이벤트 상태 추적

### 7.1 처리 원칙

본 시스템의 책임 범위는 다음과 같이 명확히 분리됩니다.

- **자동 처리 가능한 에러**: HIST 등을 분석해 정상으로 판단되면 Airflow Clear API를 직접 호출하고, 호출 결과를 로그 테이블에 기록합니다.
- **운영자 판단이 필요한 에러**: 자동 판단 불가 또는 자동 판단 결과가 비정상인 경우, 운영자가 **Airflow UI에서 직접** Clear/Mark Success 처리합니다. 이 경우 본 시스템은 "운영자 처리 필요" 상태까지만 기록하며, **이후 Airflow에서 어떤 조치가 이루어졌는지는 추적하지 않습니다.**

> 즉, 본 시스템은 "자동 조치가 가능한 케이스를 걸러서 자동화"하는 역할만 수행하며, 운영자 승인 워크플로우(승인 화면, 승인 내역 저장 등)는 가지지 않습니다.

#### 상태 기록 방식: append-only

`StatusLog`는 **이력 추가(append-only) 방식**으로 운영합니다. 상태 변화마다 row를 갱신하는 것이 아니라, 같은 `eventId`로 새 row를 추가합니다.

- 한 row = 한 시점의 상태 스냅샷 (`statusType` + `judgementType` + `reason` + `createAt`)
- 한 `eventId`에 여러 row가 시간순으로 쌓임 → 처리 흐름이 그대로 감사 로그가 됨
- "현재 상태" 조회 = 해당 `eventId`에서 가장 최근 `createAt`의 row
- `eventId`에 unique 제약을 걸지 않습니다 (`event_id + create_at` 복합 인덱스로만 조회)
- mark*() 같은 setter 메서드를 두지 않습니다. `StatusLog.builder().build()` + `statusLogUnit.create()`로 일관되게 추가합니다.

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

모든 종결 상태(`AUTO_CLEARED`, `AUTO_CLEAR_FAILED`, `MANUAL_REVIEW_REQUIRED`)의 row가 추가되면 본 시스템 내에서 더 이상의 이력은 추가되지 않습니다.

> 화살표는 "row 갱신"이 아니라 "다음 단계의 row 추가"를 의미합니다. 이전 단계 row는 그대로 보존됩니다.

### 7.3 상태 정의

각 상태는 새 row를 추가하는 시점입니다. 종결 row가 기록되면 같은 `eventId`로 더 이상 row가 추가되지 않습니다.

| 상태 | row 추가 시점 | 추가 주체 | 종결 여부 | 설명 |
|------|------|----------|----------|------|
| `RECEIVED` | 에러 수신 직후 | `StatusLogServiceImpl` | × | 첫 이력 row |
| `AUTO_VERIFYING` | 자동 검증 시작 | `KafkaLivestockErrorEventConsumer` | × | HIST 조회 등 자동 분석 진행 중 |
| `AUTO_CLEARED` | 자동 검증 정상 판단 | `KahisServiceImpl` | ✓ | 정상 판단 완료 (※ Airflow Clear API 호출은 TODO) |
| `AUTO_CLEAR_SUCCESS` | (예약) Airflow Clear API 성공 | (미구현) | ✓ | Airflow Clear API 호출 도입 시 사용 예정. 현재 미사용 |
| `AUTO_CLEAR_FAILED` | (예약) Airflow Clear API 실패 | (미구현) | ✓ | Airflow Clear API 도입 시 사용 예정. 현재 미사용 |
| `MANUAL_REVIEW_REQUIRED` | 자동 판단 불가/비정상 또는 분석 미지원 토픽 | `KahisServiceImpl` 또는 `KafkaEventConsumer` | ✓ | 운영자가 Airflow에서 직접 처리 — **이후 추적 없음** |

### 7.4 errorType별 처리 정책

`ErrorType.isNeedAnalysis` 플래그로 분석 가능 여부를 enum 자체에 박아두고, `KafkaEventConsumer`는 `notAnalysisTopics()`로 자동 산출된 토픽 목록을 한 번에 구독합니다.

| errorType | `isNeedAnalysis` | Consumer | 동작 |
|-----------|---------------|----------|------|
| `LIVESTOCK_ANOMALY` | true | `KafkaLivestockErrorEventConsumer` | HIST 조회 → 정상 시 `AUTO_CLEARED`, 그 외 `MANUAL_REVIEW_REQUIRED` |
| `PREDICTION_ANOMALY` | false | `KafkaEventConsumer` | 즉시 `MANUAL_REVIEW_REQUIRED` |
| `FARM_COUNT_ANOMALY` | false | `KafkaEventConsumer` | 즉시 `MANUAL_REVIEW_REQUIRED` |
| `PNU_ANOMALY` | false | `KafkaEventConsumer` | 즉시 `MANUAL_REVIEW_REQUIRED` |
| `FARM_COORDINATE_MISSING` | false | `KafkaEventConsumer` | 즉시 `MANUAL_REVIEW_REQUIRED` |
| `DATA_NOT_FOUND` | false | `KafkaEventConsumer` | 즉시 `MANUAL_REVIEW_REQUIRED` |
| `TRAININGSET_COUNT_MISMATCH` | false | `KafkaEventConsumer` | 즉시 `MANUAL_REVIEW_REQUIRED` |
| `CALC_ENV_ANOMALY` | false | `KafkaEventConsumer` | 즉시 `MANUAL_REVIEW_REQUIRED` |
| `UNKNOWN` | false | `KafkaEventConsumer` | 즉시 `MANUAL_REVIEW_REQUIRED` (파싱 실패 케이스 포함) |

> 향후 분석 가능 errorType이 추가되면 enum의 `isNeedAnalysis = true`로 토글하고 전용 Consumer를 추가하면 됩니다.

### 7.5 StatusLog 엔티티

append-only 모델이므로 row 갱신 가정의 필드(`statusUpdatedAt`, `clearRequestedAt`, `clearFailureReason` 등)는 두지 않습니다. Clear 결과 등 상태 부가 정보는 해당 상태 row의 `reason`에 함께 기록합니다. `eventId`는 unique가 아니라 시간순 조회를 위한 복합 인덱스(`event_id + create_at`)로만 다룹니다.

`lsfarmId`(방역본부 농장 번호)는 `KahisServiceImpl`이 `LsFarmIdFinder`로 조회해 같이 적재합니다. 분석을 수행하지 않는 row(예: `RECEIVED`, `AUTO_VERIFYING`, `KafkaEventConsumer`의 `MANUAL_REVIEW_REQUIRED`)에서는 null로 들어갑니다.

```java
@Entity
@Table(
    schema = "batch_monitor",
    name = "STATUS_LOG",
    indexes = {
        @Index(name = "IDX_STATUS_LOG_CREATE_AT", columnList = "create_at"),
        @Index(name = "IDX_STATUS_LOG_EVENT_ID_CREATE_AT", columnList = "event_id, create_at")
    }
)
@EntityListeners(AuditingEntityListener.class)
@Getter
@DynamicInsert
@NoArgsConstructor(access = PROTECTED)
@Comment("배치 모니터링 로그 관리 > 로그")
public class StatusLog {

  @Id
  @TsuId
  @Comment("일련 번호")
  private String id;

  // Kafka 이벤트 식별자 (도메인 키, 같은 eventId의 여러 row가 누적됨)
  @Column(name = "event_id", nullable = false)
  @Comment("이벤트 일련 번호. taskId-milliseconds")
  private String eventId;

  @Comment("airflow dag 일련 번호")
  private String dagId;

  @Comment("airflow task 일련 번호")
  private String taskId;

  @Comment("방역본부 농장 번호")
  private String lsfarmId;

  @Enumerated(EnumType.STRING)
  @Comment("airflow에서 발생한 에러 유형")
  private ErrorType errorType;

  @Comment("에러 메시지")
  private String errorMessage;

  @Comment("에러 메시지에서 추출한 메타 데이터")
  private String metadata;

  @Column(nullable = false)
  @Enumerated(EnumType.STRING)
  @Comment("배치 모니터링 시스템의 상태 유형")
  private StatusType statusType;

  @Enumerated(EnumType.STRING)
  @Comment("판단 유형")
  private JudgementType judgementType;

  @Comment("판단 근거")
  private String reason;

  @CreatedDate
  @Column(nullable = false, updatable = false)
  @Comment("생성 날짜 시간")
  private LocalDateTime createAt;

  @Builder
  public StatusLog(String eventId, String dagId, String taskId, String lsfarmId, ErrorType errorType,
      String errorMessage, String metadata, StatusType statusType, JudgementType judgementType,
      String reason) { /* ... */ }
}
```

> **연쇄 에러 추적 제거**: 운영자가 Airflow에서 직접 Clear한 이후의 처리는 본 시스템이 알 수 없으므로, 이전 설계에 있던 `parentEventId` / `DOWNSTREAM_FAILED` 개념은 더 이상 유지하지 않습니다. 새 에러는 항상 독립된 이벤트로 기록됩니다.

### 7.6 컴포넌트별 코드 참조

- 수신/저장/발행 흐름: 4.1 ~ 4.3 (`StatusLogController`, `StatusLogServiceImpl`, `KafkaEventProducer`)
- 사육두수 분석 Consumer: 4.4 (`KafkaLivestockErrorEventConsumer`)
- 분석 미지원 토픽 묶음 Consumer: 4.5 (`KafkaEventConsumer`)
- 분석 오케스트레이션: 4.6 (`KahisServiceImpl`)
- HIST tolerance 분석: 4.7 (`LivestockHistoryAnalyzer`)
- 농가번호 매핑: 4.8 (`LsFarmIdFinder`)
- SMS 알림: 4.9 (TODO)
