# 프로젝트 구조와 DataSource 매핑 가이드

> 이 문서는 두 데이터소스(JPA / MyBatis) 분리 원칙을 설명합니다. 실제 프로젝트 레이아웃은 **도메인 분리(`domain/airflow`, `domain/kafka`, `domain/kahis`, `domain/parser`, `domain/statuslog`)와 공용 계층(`controller`, `service`, `dto`, `vo`, `common`)을 혼합**해 사용합니다. 자세한 현재 구조는 `docs/project/spring-boot-architecture.md` Section 3 참조.

## 권장 디렉토리 구조 (도메인 분리 예시 — 참고용)

```
batchmonitor/
├── Application.java
│     └─ @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
│
├── common/config/
│     ├─ PostgresDataSourceConfig.java  ── JPA 설정 (entity, repository 스캔)
│     └─ OracleDataSourceConfig.java    ── MyBatis 설정 (mapper 스캔)
│
├── domain/
│     ├─ livestock/
│     │    ├─ entity/          ← PostgreSQL (JPA)
│     │    ├─ repository/      ← PostgreSQL (JPA)
│     │    ├─ mapper/          ← Oracle (MyBatis)
│     │    ├─ dto/             ← 양쪽 모두 사용 가능
│     │    └─ service/
│     │
│     ├─ prediction/
│     │    ├─ mapper/          ← Oracle (MyBatis)
│     │    ├─ dto/
│     │    └─ service/
│     │
│     └─ notification/
│          └─ service/
│
└── resources/
      └─ mapper/               ← MyBatis XML 매퍼 파일
           ├─ livestock/
           │    └─ LivestockMapper.xml
           └─ prediction/
                └─ PredictionMapper.xml
```

## 실제 프로젝트 레이아웃 (도메인 + 공용 계층 혼합)

현재 코드는 외부 시스템 의존성(Airflow / Kafka / Oracle 등)을 `domain/<source>` 아래로 모으고, HTTP/오케스트레이션은 상위 공용 계층에 두는 구조입니다:

- `controller/` — HTTP 진입점 (`StatusLogController`)
- `service/` — 오케스트레이션 (`StatusLogServiceImpl`, `KahisServiceImpl`)
- `domain/airflow/{client,dto}/` — Airflow REST 호출 (Feign 클라이언트 + 요청/응답 DTO)
- `domain/kafka/{producer,consumer,dto}/` — Kafka Producer/Consumer + 메시지 DTO
- `domain/kahis/{mapper,dto}/` — Oracle MyBatis 매퍼 + 조회 DTO (외부 DB 읽기 전용)
- `domain/parser/` — 에러 메시지 정규식 파싱 (`ParserUtil` + 패턴/데이터 record)
- `domain/statuslog/{entity,repository,unit,enumeration}/` — PostgreSQL JPA + 영속 단위
- `vo/` — 외부 의존(매퍼) + 도메인 로직 결합 컴포넌트 (`LivestockHistoryAnalyzer`, `LsFarmIdFinder`)
- `dto/{request,data}/` — 컨트롤러 요청 / 내부 데이터 record
- `common/{annotation,config,enumeration,extension,generator}/` — 공통 인프라

**JPA vs MyBatis 매핑 규칙**은 위 도메인 분리 예시와 동일합니다 (아래 섹션 참조). 패키지 위치만 `domain/xxx/repository`가 아니라 `domain/statuslog/repository`, `domain/kahis/mapper`로 도메인별로 정리되어 있는 차이만 있습니다.

## 어디에 뭘 넣어야 하는가

### PostgreSQL에 저장하는 것 (자체 DB — JPA)

에러 상태 관리처럼 **우리가 만들고 관리하는 데이터**:

| 폴더 | 파일 | 설명 |
|------|------|------|
| `domain/xxx/entity/` | `ErrorEvent.java` | `@Entity` 클래스 |
| `domain/xxx/repository/` | `ErrorEventRepository.java` | `JpaRepository<>` 인터페이스 |

```java
// entity/ — @Entity가 있으면 PostgresDataSourceConfig의 EntityManagerFactory가 관리
@Entity
@Table(name = "error_event")
public class ErrorEvent { ... }

// repository/ — JpaRepository를 상속하면 PostgresDataSourceConfig의 @EnableJpaRepositories가 스캔
public interface ErrorEventRepository extends JpaRepository<ErrorEvent, Long> { ... }
```

### Oracle에서 조회하는 것 (고객사 DB — MyBatis)

고객사 테이블에서 **읽기만 하는 데이터**:

| 폴더 | 파일 | 설명 |
|------|------|------|
| `domain/xxx/mapper/` | `LivestockMapper.java` | `@Mapper` 인터페이스 |
| `domain/xxx/dto/` | `MobileBlvstckHist.java` | 조회 결과 DTO |
| `resources/mapper/xxx/` | `LivestockMapper.xml` | SQL 작성 |

```java
// mapper/ — @Mapper가 있으면 OracleDataSourceConfig의 @MapperScan이 스캔
@Mapper
public interface LivestockMapper {
    MobileBlvstckHist selectLatestHist(@Param("frmhsNo") String frmhsNo);
}
```

### 판단 기준 요약

```
새 파일을 만들 때:
  └─ 이 데이터를 우리가 저장/수정하는가?
       ├─ YES → entity/ + repository/ (PostgreSQL, JPA)
       └─ NO (고객사 DB에서 읽기만) → mapper/ + dto/ (Oracle, MyBatis)
```

## 스캔 경로 분리 방식

현재 두 Config는 **서로 다른 하위 패키지를 명시적으로 지정**해 충돌 가능성을 처음부터 차단합니다:

```
PostgresDataSourceConfig:
  @EnableJpaRepositories(basePackages = "kr.go.kahis.batchmonitor.domain.statuslog.repository")
  EntityManagerFactory.packages("kr.go.kahis.batchmonitor.domain.statuslog.entity")

OracleDataSourceConfig:
  @MapperScan(basePackages = "kr.go.kahis.batchmonitor.domain.kahis.mapper")
```

마커(`JpaRepository` 상속, `@Mapper` 어노테이션)로 구분되므로 같은 패키지에 섞여 있어도 안전하지만, 도메인을 패키지 단위로 나눠 둔 덕분에 어떤 도메인을 어떤 데이터소스가 관리하는지 코드만 봐도 명확합니다.

## 새 도메인 추가 시 체크리스트

예: `farm` 도메인을 추가한다면:

1. `domain/farm/` 패키지 생성
2. Oracle 조회가 필요하면:
   - `domain/farm/mapper/FarmMapper.java` — `@Mapper` 인터페이스
   - `domain/farm/dto/FarmInfo.java` — 조회 결과 DTO
   - `resources/mapper/farm/FarmMapper.xml` — SQL
   - `OracleDataSourceConfig`의 `@MapperScan(basePackages = ...)`에 새 패키지 추가 (또는 상위 `domain` 단위로 확장)
3. PostgreSQL 저장이 필요하면:
   - `domain/farm/entity/FarmSyncLog.java` — `@Entity` 클래스
   - `domain/farm/repository/FarmSyncLogRepository.java` — JPA Repository
   - `PostgresDataSourceConfig`의 `@EnableJpaRepositories(basePackages = ...)` 및 `EntityManagerFactory.packages(...)`에 새 패키지 추가
4. 현재 Config는 `domain.kahis.mapper` / `domain.statuslog.{repository,entity}`만 명시적으로 스캔하므로 도메인이 추가될 때마다 Config의 스캔 경로를 함께 갱신해야 합니다.
