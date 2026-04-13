package kr.go.kahis.batchmonitor.persistence.repository;

import kr.go.kahis.batchmonitor.persistence.entity.StatusLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StatusLogRepository extends JpaRepository<StatusLog, String> {

}
