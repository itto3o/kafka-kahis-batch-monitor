package kr.go.kahis.batchmonitor.domain.statuslog.repository;

import kr.go.kahis.batchmonitor.domain.statuslog.entity.StatusLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StatusLogRepository extends JpaRepository<StatusLog, String> {

}
