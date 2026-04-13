# PostgreSQL + JPA 설정 상세

## yml 설정

```yaml
spring:
  datasource:
    postgres:
      url: jdbc:postgresql://localhost:5432/postgres
      username: postgres
      password: postgres
      driver-class-name: org.postgresql.Driver
  jpa:
    hibernate:
      ddl-auto: update      # 엔티티 기반으로 테이블 자동 생성/수정
    show-sql: true           # 실행되는 SQL을 로그에 출력
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
```

`spring.datasource.postgres`는 Spring Boot의 기본 경로가 아니다.
기본은 `spring.datasource`인데, Oracle과 분리하기 위해 하위 경로로 나눈 것이다.
이 경로는 `@ConfigurationProperties`에서 매핑한다.

## Config 클래스 한 줄씩 해석

```java
@Configuration  // Spring이 이 클래스를 설정 클래스로 인식한다
@EnableJpaRepositories(
    basePackages = "kr.go.kahis.batchmonitor.domain",  // 이 패키지 하위의 JpaRepository를 스캔
    entityManagerFactoryRef = "postgresEntityManagerFactory",  // 어떤 EntityManagerFactory를 쓸지
    transactionManagerRef = "postgresTransactionManager"       // 어떤 TransactionManager를 쓸지
)
public class PostgresDataSourceConfig {
```

### 1단계: 접속 정보 로드

```java
@Primary
@Bean
@ConfigurationProperties("spring.datasource.postgres")
public DataSourceProperties postgresDataSourceProperties() {
    return new DataSourceProperties();
}
```

- `@ConfigurationProperties("spring.datasource.postgres")` — yml의 해당 경로 아래 값들을 `DataSourceProperties` 객체에 자동 바인딩한다
- 빈 `DataSourceProperties`를 리턴하면, Spring이 yml 값을 setter로 채워준다

```
spring.datasource.postgres.url       → DataSourceProperties.setUrl()
spring.datasource.postgres.username  → DataSourceProperties.setUsername()
spring.datasource.postgres.password  → DataSourceProperties.setPassword()
```

### 2단계: DataSource 생성

```java
@Primary
@Bean
public DataSource postgresDataSource() {
    return postgresDataSourceProperties()
        .initializeDataSourceBuilder()
        .build();
}
```

- `initializeDataSourceBuilder()` — Properties에 담긴 url, username, password, driver로 DataSource 빌더를 만든다
- `build()` — 실제 커넥션 풀(HikariCP)을 생성한다

### 3단계: EntityManagerFactory 생성

```java
@Primary
@Bean
public LocalContainerEntityManagerFactoryBean postgresEntityManagerFactory(
        EntityManagerFactoryBuilder builder) {
    return builder
        .dataSource(postgresDataSource())                        // 이 DataSource를 사용
        .packages("kr.go.kahis.batchmonitor.domain")            // @Entity 클래스를 스캔할 패키지
        .persistenceUnit("postgres")                             // persistence unit 이름 (구분용)
        .build();
}
```

- `EntityManagerFactory`는 JPA의 핵심 — Entity를 관리하고 SQL을 생성하는 Hibernate 세션을 만든다
- `packages()`에 지정한 패키지 하위의 `@Entity` 클래스만 이 Factory가 관리한다

### 4단계: TransactionManager 연결

```java
@Primary
@Bean
public PlatformTransactionManager postgresTransactionManager(
        EntityManagerFactory postgresEntityManagerFactory) {
    return new JpaTransactionManager(postgresEntityManagerFactory);
}
```

- `@Transactional`이 붙은 메서드에서 이 TransactionManager가 사용된다
- `@Primary`이므로 `@Transactional`만 쓰면 자동으로 PostgreSQL 트랜잭션이 적용된다

## 이 프로젝트에서의 사용 예시

### Entity 정의 (domain/livestock/entity/)

```java
package kr.go.kahis.batchmonitor.domain.livestock.entity;

@Entity
@Table(name = "error_event")
public class ErrorEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    private ErrorStatus status;  // PENDING, APPROVED, ACTION_EXECUTED ...

    private String errorType;
    private String dagId;
    private String taskId;
    private LocalDateTime createdAt;
}
```

### Repository 정의 (domain/livestock/repository/)

```java
package kr.go.kahis.batchmonitor.domain.livestock.repository;

public interface ErrorEventRepository extends JpaRepository<ErrorEvent, Long> {

    List<ErrorEvent> findByStatus(ErrorStatus status);
}
```

별도 설정 없이 `@EnableJpaRepositories`가 `domain` 패키지를 스캔하므로 자동 등록된다.

### Service에서 사용

```java
@Service
@RequiredArgsConstructor
public class ErrorEventService {

    private final ErrorEventRepository errorEventRepository;

    @Transactional  // @Primary이므로 postgresTransactionManager가 적용됨
    public void approve(Long id) {
        ErrorEvent event = errorEventRepository.findById(id).orElseThrow();
        event.setStatus(ErrorStatus.APPROVED);
    }
}
```
