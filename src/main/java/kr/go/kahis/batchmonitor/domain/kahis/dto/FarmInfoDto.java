package kr.go.kahis.batchmonitor.domain.kahis.dto;

import java.time.LocalDateTime;

public record FarmInfoDto(

  // FARM_NO. LSFARM 농장 번호
  Integer farmNo,

  // FARM_NM. 농장명 (1차 식별 검증)
  String farmNm,

  // BRD_OWNER_NM. 축주명 (1차 식별 검증)
  String brdOwnerNm,

  // FRMHS_STTUS_CODE. 농가 운영상태 (폐업이면 두수 변동 무의미)
  String frmhsSttusCode,

  // RE_PREARNGE_DE. 재입식 예정 일자 (직후의 큰 두수 점프는 정상)
  LocalDateTime rePrearngeDe,

  // UPDT_DE. 수정 일자
  LocalDateTime updtDe
) {

}
