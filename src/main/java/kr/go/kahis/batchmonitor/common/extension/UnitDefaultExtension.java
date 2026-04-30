package kr.go.kahis.batchmonitor.common.extension;

/**
 * 기본 Unit 기능 interface
 *
 * @param <E>  Entity
 * @param <ID> ID 데이터 유형
 * @author 류성재
 * @apiNote Unit 공통 기능을 정의
 */
public interface UnitDefaultExtension<E, ID> {

  /**
   * 데이터 등록
   *
   * @param entity Entity
   * @return id
   * @apiNote 데이터 등록
   * @author 류성재
   */
  ID create(E entity);

  /**
   * 데이터 존재 여부
   *
   * @param id 일련 번호
   * @return boolean
   * @apiNote 데이터 존재 여부
   * @author 류성재
   */
  Boolean exists(ID id);

  /**
   * 데이터 조회
   *
   * @param id 일련 번호
   * @return e
   * @apiNote 데이터 조회
   * @author 류성재
   */
  E get(ID id);

}
