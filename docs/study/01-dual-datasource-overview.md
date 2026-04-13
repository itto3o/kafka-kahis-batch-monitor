# Dual DataSource 개요

## 왜 DataSource가 2개 필요한가?

이 프로젝트는 두 개의 DB를 사용한다:

| 구분 | PostgreSQL | Oracle |
|------|-----------|--------|
| 용도 | 에러 상태 관리 (자체 DB) | 고객사 데이터 조회 (읽기 전용) |
| 접근 기술 | JPA (Spring Data) | MyBatis |
| 권한 | DDL/DML 모두 가능 | SELECT만 가능 |

Spring Boot는 기본적으로 **하나의 DataSource만 자동 설정**한다.
두 개 이상의 DB를 연결하려면 자동 설정을 끄고, 각각 수동으로 구성해야 한다.

## 전체 구조

```
Application.java
  └─ @SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
         ↑ 자동 설정을 꺼야 수동 설정이 충돌하지 않는다

common/config/
  ├─ PostgresDataSourceConfig.java   ← JPA용 (상태 관리)
  └─ OracleDataSourceConfig.java     ← MyBatis용 (고객사 조회)
```

## 흐름 요약

```
application-dev.yml
  spring.datasource.postgres.*  ──→  PostgresDataSourceConfig  ──→  JPA Repository
  spring.datasource.oracle.*    ──→  OracleDataSourceConfig    ──→  MyBatis Mapper
```

각 Config 클래스가 하는 일:
1. yml에서 접속 정보를 읽는다 (`DataSourceProperties`)
2. 접속 정보로 `DataSource`를 만든다
3. 해당 DataSource를 사용하는 ORM 인프라를 구성한다 (JPA: EntityManager / MyBatis: SqlSessionFactory)
4. 트랜잭션 매니저를 연결한다

## @Primary의 역할

두 DataSource가 있으면 Spring은 어느 것을 주입해야 할지 모른다.
`@Primary`가 붙은 쪽이 **기본값**이 된다.

```java
@Primary  // 다른 곳에서 DataSource를 주입받을 때 이것이 기본으로 선택됨
@Bean
public DataSource postgresDataSource() { ... }

@Bean  // Primary가 아니므로 명시적으로 지정할 때만 사용됨
public DataSource oracleDataSource() { ... }
```

이 프로젝트에서는 PostgreSQL이 Primary다 — JPA, 트랜잭션 등에서 별도 지정 없이 사용되는 기본 DB라는 뜻이다.
