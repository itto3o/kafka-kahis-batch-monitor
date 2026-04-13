# 프로젝트 구조와 DataSource 매핑 가이드

## 전체 디렉토리 구조

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

## 스캔 경로가 겹쳐도 괜찮은 이유

두 Config 모두 `basePackages = "kr.go.kahis.batchmonitor.domain"`을 스캔한다.
하지만 각각 다른 마커를 찾는다:

```
@EnableJpaRepositories  →  JpaRepository를 상속한 인터페이스만 등록
@MapperScan             →  @Mapper가 붙은 인터페이스만 등록
```

같은 패키지 안에 `ErrorEventRepository`(JPA)와 `LivestockMapper`(MyBatis)가 있어도:
- `ErrorEventRepository`는 JPA가 가져감 (JpaRepository 상속)
- `LivestockMapper`는 MyBatis가 가져감 (@Mapper 어노테이션)

서로의 대상에 관심이 없으므로 충돌하지 않는다.

## 새 도메인 추가 시 체크리스트

예: `farm` 도메인을 추가한다면:

1. `domain/farm/` 패키지 생성
2. Oracle 조회가 필요하면:
   - `domain/farm/mapper/FarmMapper.java` — `@Mapper` 인터페이스
   - `domain/farm/dto/FarmInfo.java` — 조회 결과 DTO
   - `resources/mapper/farm/FarmMapper.xml` — SQL
3. PostgreSQL 저장이 필요하면:
   - `domain/farm/entity/FarmSyncLog.java` — `@Entity` 클래스
   - `domain/farm/repository/FarmSyncLogRepository.java` — JPA Repository
4. **Config 수정은 필요 없다** — `domain` 패키지를 통째로 스캔하고 있으므로
