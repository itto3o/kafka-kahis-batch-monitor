# Kafka Batch Re-Execution

Kafka를 활용한 배치 재실행 프로그램으로, 실패한 배치 작업을 Kafka 메시지 기반으로 재처리하는 Spring Boot 애플리케이션입니다.

## 기술 스택

| 구분 | 기술 |
|------|------|
| Framework | Spring Boot 3.5.13, Spring Cloud 2025.0.1 |
| Language | Java 17 |
| Messaging | Apache Kafka (KRaft 3-node 클러스터) |
| Database | PostgreSQL |
| HTTP Client | Spring Cloud OpenFeign (Airflow API 연동) |
| Build | Gradle |
| Infra | Docker Compose |

## 아키텍처

```
┌──────────────┐      ┌──────────────────────┐      ┌──────────────┐
│   Airflow    │◄────►│  Spring Boot App     │◄────►│  PostgreSQL  │
│  (REST API)  │      │                      │      │              │
└──────────────┘      │  ┌────────────────┐  │      └──────────────┘
                      │  │ Kafka Producer │  │
                      │  └───────┬────────┘  │
                      │          │           │
                      │  ┌───────▼────────┐  │
                      │  │ Kafka Consumer │  │
                      │  └────────────────┘  │
                      └──────────┬───────────┘
                                 │
                      ┌──────────▼───────────┐
                      │   Kafka Cluster      │
                      │  (3-node KRaft)      │
                      │  :9092 :9094 :9096   │
                      └──────────────────────┘
```

## 주요 구성

### Kafka 설정

- **클러스터**: KRaft 모드 3-node 구성 (Zookeeper 불필요)
- **Consumer**: 수동 ACK (`manual_immediate`), auto-commit 비활성화, `earliest` offset 정책
- **Producer**: `acks=all` (모든 ISR 복제 확인), 최대 3회 재시도
- **직렬화**: Key - String, Value - JSON (`kr.go.kahis.*` 패키지 신뢰)

### Airflow 연동

- OpenFeign 클라이언트를 통해 Airflow REST API (`/api/v1`)와 통신
- 배치 DAG 트리거 및 실행 상태 모니터링
- 타임아웃: connect/read 각 5초
- 인증: 환경변수 `AIRFLOW_USER`, `AIRFLOW_PWD` (기본값: admin/admin)

### 데이터베이스

- PostgreSQL을 통한 배치 작업 이력 및 상태 관리
- JPA/Hibernate DDL auto-update 모드 (개발 환경)

## 실행 방법

### 1. 인프라 실행

```bash
docker compose up -d
```

다음 서비스가 시작됩니다:
- **PostgreSQL**: `localhost:5432`
- **Kafka Node 1**: `localhost:9092`
- **Kafka Node 2**: `localhost:9094`
- **Kafka Node 3**: `localhost:9096`

### 2. 애플리케이션 실행

```bash
./gradlew bootRun
```

### 3. 환경변수 (선택)

| 변수명 | 설명 | 기본값 |
|--------|------|--------|
| `AIRFLOW_USER` | Airflow API 사용자명 | admin |
| `AIRFLOW_PWD` | Airflow API 비밀번호 | admin |

## 프로젝트 구조

```
kafka-practice/
├── src/main/java/kr/go/kahis/kafkapractice/
│   └── Application.java              # Spring Boot 메인 클래스
├── src/main/resources/
│   ├── application.yml                # 공통 설정 (Jackson, Feign, Airflow)
│   └── application-local.yml          # 로컬 환경 설정 (Kafka, DB, Logging)
├── compose.yaml                       # Docker Compose (Kafka 클러스터 + PostgreSQL)
├── build.gradle                       # 빌드 설정 및 의존성
└── settings.gradle                    # Gradle 프로젝트 설정
```