package kr.go.kahis.batchmonitor.domain.kahis.dto;

import java.time.LocalDateTime;

public record FarmScaleDetailDto(
  // FARM_NO. LSFARM 농장 번호
  Integer farmNo,

  // UPPER_PRDLST_CODE. 축종 코드
  String upperPrdlstCode,

  // SPCIES_CODE. 품종 코드 (에러 로그의 6자리 코드와 매칭)
  String spciesCode,

  // SEQ_SN. 일련번호 (PK 일부, 같은 품종 다중 행 식별)
  Long seqSn,

  // BRD_CO. 사육 두수 (당일 시점)
  Long brdCo,

  // AVG_CNT. 현행화 상시 두수 (변동성 판단 핵심 — 가금/한우 종 특성 반영)
  Long avgCnt,

  // MXMM_BRD_CO. 최대 사육 두수 (오기입 1차 필터: BRD_CO > MXMM_BRD_CO 면 이상)
  Long mxmmBrdCo,

  // MOTHER_PIG_CO. 모돈 수 (돼지 한정 세분화)
  Long motherPigCo,

  // PORKER_CO. 비육돈 수 (돼지 한정 세분화)
  Long porkerCo,

  // MGR_CODE. 현행화 운영상태
  String mgrCode,

  // KEEP_CODE. 현행화 사육방식
  String keepCode,

  // EXAMIN_DE. 조사 일자
  LocalDateTime examinDe,

  // UPDT_DE. 수정 일자 (polling 기준)
  LocalDateTime updtDe
) {

}
