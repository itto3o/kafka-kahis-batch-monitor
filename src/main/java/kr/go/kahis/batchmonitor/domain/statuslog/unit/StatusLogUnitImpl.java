package kr.go.kahis.batchmonitor.domain.statuslog.unit;

import jakarta.persistence.EntityNotFoundException;
import kr.go.kahis.batchmonitor.common.annotation.Unit;
import kr.go.kahis.batchmonitor.domain.statuslog.entity.StatusLog;
import kr.go.kahis.batchmonitor.domain.statuslog.repository.StatusLogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.transaction.annotation.Transactional;

@Unit
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class StatusLogUnitImpl implements StatusLogUnit {

  private final StatusLogRepository repository;

  /**
   * 데이터 등록
   *
   * @param entity Entity
   * @return id
   * @apiNote 데이터 등록
   * @author 류성재
   */
  @Override
  @Transactional
  public String create(StatusLog entity) {
    return repository.save(entity).getId();
  }

  /**
   * 데이터 존재 여부
   *
   * @param id 일련 번호
   * @return boolean
   * @apiNote 데이터 존재 여부
   * @author 류성재
   */
  @Override
  public Boolean exists(String id) {
    return repository.existsById(id);
  }

  /**
   * 데이터 조회
   *
   * @param id 일련 번호
   * @return e
   * @apiNote 데이터 조회
   * @author 류성재
   */
  @Override
  public StatusLog get(String id) {
    return repository.findById(id).orElseThrow(EntityNotFoundException::new);
  }
}
