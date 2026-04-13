# TsuId: TSID 기반 커스텀 ID 생성기

## 1. TSID란?

TSID(Time-Sorted Unique Identifier)는 시간 순서가 보장되는 고유 식별자다.

```
TSID 내부 구조 (64-bit):
┌──────────────────────────────┬──────────────────┐
│     timestamp (42 bits)       │  random (22 bits) │
└──────────────────────────────┴──────────────────┘
```

기존 ID 방식과 비교:

| 방식 | 정렬 가능 | 고유성 | 크기 | 외부 노출 안전성 |
|------|----------|--------|------|-----------------|
| AUTO_INCREMENT | O | O (단일 DB) | 작음 | X (예측 가능) |
| UUID v4 | X (랜덤) | O | 큼 (36자) | O |
| TSID | O (시간순) | O | 중간 (13자) | O |

TSID의 장점:
- **시간순 정렬** — INSERT 순서가 곧 ID 순서, 인덱스 성능이 좋음
- **UUID보다 짧음** — 13자 문자열 (UUID는 36자)
- **예측 불가** — AUTO_INCREMENT처럼 다음 값을 추측할 수 없음
- **분산 환경 안전** — DB 없이 애플리케이션에서 생성, 충돌 없음

## 2. 이 프로젝트의 커스텀: TsuId

hypersistence-utils 라이브러리의 TSID를 기반으로, **prefix + epoch + TSID**를 조합하는 커스텀 ID 생성기를 만들었다.

### 구성 파일

```
common/
├── annotation/
│   └── TsuId.java           ← 어노테이션 (설정값 정의)
└── generator/
    └── TsuIdGenerator.java  ← 실제 ID 생성 로직
```

### 기본 사용법

```java
@Id
@TsuId
private String id;
// 결과: "1744185600000-0GWKZ6KBCTR4R"
//        └── epoch(ms) ──┘└── TSID ──┘
```

### 옵션 커스텀

```java
// prefix 추가
@Id
@TsuId(prefix = "ERR")
private String id;
// 결과: "ERR-1744185600000-0GWKZ6KBCTR4R"

// prefix + epoch 없이
@Id
@TsuId(prefix = "LOG", useEpoch = false)
private String id;
// 결과: "LOG-0GWKZ6KBCTR4R"

// 구분자 변경
@Id
@TsuId(prefix = "ERR", separate = "_")
private String id;
// 결과: "ERR_1744185600000_0GWKZ6KBCTR4R"

// Long 타입 (prefix, epoch 무시됨)
@Id
@TsuId
private Long id;
// 결과: 561898838919168 (TSID의 long 값 그대로)
```

## 3. @TsuId 어노테이션 상세

```java
@IdGeneratorType(TsuIdGenerator.class)          // Hibernate에 이 Generator를 사용하라고 등록
@ValueGenerationType(generatedBy = TsidValueGenerator.class)  // @Id가 아닌 필드에도 사용 가능
@Retention(RetentionPolicy.RUNTIME)
@Target({FIELD, METHOD})
public @interface TsuId {

    // TSID Factory 공급자 (기본: 스레드 안전한 싱글톤)
    Class<? extends Supplier<Factory>> value() default FactorySupplier.class;

    String prefix() default "";       // ID 앞에 붙는 접두어 (String 타입만)
    String separate() default "-";    // 구분 문자 (String 타입만)
    boolean useEpoch() default true;  // epoch 밀리초 포함 여부 (String 타입만)
}
```

### FactorySupplier

```java
class FactorySupplier implements Supplier<TSID.Factory> {

    public static final FactorySupplier INSTANCE = new FactorySupplier();

    private final TSID.Factory tsidFactory = TSID.Factory.builder()
        .withRandomFunction(TSID.Factory.THREAD_LOCAL_RANDOM_FUNCTION)  // 스레드별 Random
        .build();
}
```

- **싱글톤** — `INSTANCE`로 전체 애플리케이션에서 하나의 Factory만 사용
- **THREAD_LOCAL_RANDOM_FUNCTION** — 멀티스레드 환경에서 락 없이 안전하게 생성

## 4. TsuIdGenerator 동작 원리

```java
public class TsuIdGenerator implements IdentifierGenerator {

    // 생성자에서 필드 타입을 감지
    public TsuIdGenerator(TsuId config, Member member, CustomIdGeneratorCreationContext context) {
        attributeType = AttributeType.valueOf(ReflectionUtils.getMemberType(member));
        //                                    └── Long? String? TSID? 자동 판별
    }

    @Override
    public Object generate(...) {
        return attributeType.cast(prefix, separate, useEpoch, factory.generate());
    }
}
```

### 필드 타입별 생성 결과

```
AttributeType.LONG:
  factory.generate() → tsid.toLong()
  결과: 561898838919168

AttributeType.STRING:
  factory.generate() → prefix + separate + epoch + separate + tsid.toString()
  결과: "ERR-1744185600000-0GWKZ6KBCTR4R"

AttributeType.TSID:
  factory.generate() → tsid (원본 그대로)
  결과: TSID 객체
```

String 타입일 때의 조합 과정:

```
@TsuId(prefix = "ERR", separate = "-", useEpoch = true)
private String id;

1. prefix가 있으면:  "ERR" + "-"
2. useEpoch가 true:  "ERR-" + "1744185600000" + "-"
3. TSID 추가:        "ERR-1744185600000-" + "0GWKZ6KBCTR4R"

최종: "ERR-1744185600000-0GWKZ6KBCTR4R"
```

## 5. 이 프로젝트에서의 활용

### StatusLog 엔티티

```java
@Entity
@Table(schema = "batch_monitor", name = "STATUS_LOG")
public class StatusLog {

    @Id
    @TsuId
    private String id;
    // prefix 없이 기본 설정 → "1744185600000-0GWKZ6KBCTR4R"
}
```

### 왜 TSID를 선택했는가

이 프로젝트는 에러 이벤트를 시간순으로 추적하는 것이 핵심이다:

1. **시간순 정렬이 자연스러움** — 에러 발생 순서대로 ID가 생성되므로 `ORDER BY id`만으로 시간순 조회 가능
2. **분산 생성 가능** — Kafka Consumer가 여러 개여도 ID 충돌 없음
3. **외부 노출 안전** — eventId로 REST API에 노출해도 다음 ID를 추측할 수 없음
4. **인덱스 성능** — 시간순 삽입이므로 B-Tree 인덱스에서 페이지 분할이 적음 (UUID는 랜덤 삽입이라 성능 저하)

### prefix 활용 예시

향후 엔티티가 추가된다면 prefix로 구분할 수 있다:

```java
// 에러 이벤트
@Id @TsuId(prefix = "ERR")
private String id;  // "ERR-1744185600000-0GWKZ6KBCTR4R"

// 상태 로그
@Id @TsuId(prefix = "LOG")
private String id;  // "LOG-1744185600000-0GWKZ6KBCTR4R"

// 알림 이력
@Id @TsuId(prefix = "NTF")
private String id;  // "NTF-1744185600000-0GWKZ6KBCTR4R"
```

ID만 보고도 어떤 엔티티인지 알 수 있고, 로그에서 추적이 쉬워진다.

## 6. 의존성

```groovy
// build.gradle
implementation 'io.hypersistence:hypersistence-utils-hibernate-63:3.10.3'
```

이 라이브러리가 제공하는 것:
- `TSID`, `TSID.Factory` — TSID 생성 핵심
- `TsidValueGenerator` — `@ValueGenerationType`용 Generator
- `ReflectionUtils` — 필드 타입 감지

커스텀으로 만든 것:
- `@TsuId` 어노테이션 — prefix, separate, useEpoch 옵션 추가
- `TsuIdGenerator` — 필드 타입별 분기 + String 조합 로직