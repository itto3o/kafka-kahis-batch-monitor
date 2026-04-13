# Oracle + MyBatis 설정 상세

## yml 설정

```yaml
spring:
  datasource:
    oracle:
      url: jdbc:oracle:thin:@localhost:1521/m2msys
      username: m2msys
      password: m2msys
      driver-class-name: oracle.jdbc.OracleDriver
```

JPA 설정(`spring.jpa.*`)은 여기에 없다 — Oracle은 MyBatis로 접근하기 때문이다.

## Config 클래스 한 줄씩 해석

```java
@Configuration
@MapperScan(
    basePackages = "kr.go.kahis.batchmonitor.domain",  // 이 패키지 하위의 @Mapper 인터페이스를 스캔
    sqlSessionFactoryRef = "oracleSqlSessionFactory"     // 어떤 SqlSessionFactory를 쓸지
)
public class OracleDataSourceConfig {
```

### @MapperScan vs @EnableJpaRepositories

| 어노테이션 | 대상 | 스캔 기준 |
|-----------|------|----------|
| `@EnableJpaRepositories` | JPA Repository | `JpaRepository`를 상속한 인터페이스 |
| `@MapperScan` | MyBatis Mapper | `@Mapper`가 붙은 인터페이스 |

둘 다 `basePackages = "...domain"`으로 같은 패키지를 스캔하지만, 서로 다른 어노테이션/인터페이스를 찾기 때문에 충돌하지 않는다.

### 1단계: 접속 정보 로드

```java
@Bean
@ConfigurationProperties("spring.datasource.oracle")
public DataSourceProperties oracleDataSourceProperties() {
    return new DataSourceProperties();
}
```

PostgreSQL과 동일한 패턴이지만, `@Primary`가 없다.
`@Primary`는 하나의 타입에 하나만 붙일 수 있다.

### 2단계: DataSource 생성

```java
@Bean
public DataSource oracleDataSource() {
    return oracleDataSourceProperties()
        .initializeDataSourceBuilder()
        .build();
}
```

### 3단계: SqlSessionFactory 생성 (JPA의 EntityManagerFactory에 해당)

```java
@Bean
public SqlSessionFactory oracleSqlSessionFactory() throws Exception {
    SqlSessionFactoryBean factory = new SqlSessionFactoryBean();
    factory.setDataSource(oracleDataSource());
    factory.setMapperLocations(
        new PathMatchingResourcePatternResolver()
            .getResources("classpath:mapper/**/*.xml")  // XML 매퍼 파일 위치
    );

    org.apache.ibatis.session.Configuration config =
        new org.apache.ibatis.session.Configuration();
    config.setMapUnderscoreToCamelCase(true);  // DB의 SNAKE_CASE → Java의 camelCase 자동 변환
    factory.setConfiguration(config);

    return factory.getObject();
}
```

`SqlSessionFactory`는 MyBatis의 핵심이다:
- SQL 세션을 생성한다
- XML 매퍼 파일을 파싱한다
- 결과를 Java 객체로 매핑한다

`mapUnderscoreToCamelCase` 설정이 중요하다:
```
DB 컬럼: FRMHS_NO  →  Java 필드: frmhsNo
DB 컬럼: USER_ID   →  Java 필드: userId
```

### 4단계: TransactionManager 연결

```java
@Bean
public PlatformTransactionManager oracleTransactionManager() {
    return new DataSourceTransactionManager(oracleDataSource());
}
```

JPA는 `JpaTransactionManager`를 쓰고, MyBatis는 `DataSourceTransactionManager`를 쓴다.
Oracle은 읽기 전용이라 트랜잭션이 크게 중요하지 않지만, 명시적으로 사용하려면:

```java
@Transactional("oracleTransactionManager")  // 이름을 명시해야 한다 (Primary가 아니므로)
public void someOracleWork() { ... }
```

## 이 프로젝트에서의 사용 예시

### Mapper 인터페이스 (domain/livestock/mapper/)

```java
package kr.go.kahis.batchmonitor.domain.livestock.mapper;

@Mapper
public interface LivestockMapper {

    MobileBlvstckHist selectLatestHist(@Param("frmhsNo") String frmhsNo,
                                       @Param("lstkspCl") String lstkspCl);
}
```

### XML 매퍼 (resources/mapper/livestock/)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN"
    "http://mybatis.org/dtd/mybatis-3-mapper.dtd">

<mapper namespace="kr.go.kahis.batchmonitor.domain.livestock.mapper.LivestockMapper">

    <select id="selectLatestHist" resultType="kr.go.kahis.batchmonitor.domain.livestock.dto.MobileBlvstckHist">
        SELECT FRMHS_NO,
               LSTKSP_CL,
               CHANGE_DT
          FROM TN_MOBILE_BLVSTCK_HIST
         WHERE FRMHS_NO = #{frmhsNo}
           AND LSTKSP_CL = #{lstkspCl}
         ORDER BY CHANGE_DT DESC
         FETCH FIRST 1 ROWS ONLY
    </select>

</mapper>
```

주의: `namespace`는 Mapper 인터페이스의 FQCN(전체 패키지 경로)과 **정확히 일치**해야 한다.

### DTO (domain/livestock/dto/)

```java
package kr.go.kahis.batchmonitor.domain.livestock.dto;

@Getter
@NoArgsConstructor
public class MobileBlvstckHist {
    private String frmhsNo;    // DB: FRMHS_NO  (mapUnderscoreToCamelCase 자동 변환)
    private String lstkspCl;   // DB: LSTKSP_CL
    private LocalDateTime changeDt;  // DB: CHANGE_DT
}
```

### Service에서 사용

```java
@Service
@RequiredArgsConstructor
public class LivestockVerificationService {

    private final LivestockMapper livestockMapper;  // MyBatis Mapper 주입

    public boolean hasRecentChange(String frmhsNo, String lstkspCl) {
        MobileBlvstckHist hist = livestockMapper.selectLatestHist(frmhsNo, lstkspCl);
        return hist != null;
    }
}
```
