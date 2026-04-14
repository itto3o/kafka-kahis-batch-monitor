package kr.go.kahis.batchmonitor.persistence.entity;

import static lombok.AccessLevel.PROTECTED;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import kr.go.kahis.batchmonitor.common.annotation.TsuId;
import kr.go.kahis.batchmonitor.persistence.enumeration.ErrorType;
import kr.go.kahis.batchmonitor.persistence.enumeration.JudgementType;
import kr.go.kahis.batchmonitor.persistence.enumeration.StatusType;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Comment;
import org.hibernate.annotations.DynamicInsert;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

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

  @Column(name = "event_id", nullable = false)
  @Comment("이벤트 일련 번호. taskId-milliseconds")
  private String eventId;

  @Comment("airflow dag 일련 번호")
  private String dagId;

  @Comment("airflow task 일련 번호")
  private String taskId;

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
  public StatusLog(String eventId, String dagId, String taskId, ErrorType errorType,
      String errorMessage, String metadata, StatusType statusType, JudgementType judgementType,
      String reason) {
    this.eventId = eventId;
    this.dagId = dagId;
    this.taskId = taskId;
    this.errorType = errorType;
    this.errorMessage = errorMessage;
    this.metadata = metadata;
    this.statusType = statusType;
    this.judgementType = judgementType;
    this.reason = reason;
  }
}
