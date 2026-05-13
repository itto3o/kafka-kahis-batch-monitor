package kr.go.kahis.batchmonitor.domain.kahis.dto;

import java.time.LocalDateTime;

public record FarmScaleDto(
  // FARM_NO. LSFARM 농장 번호
  Integer farmNo,

  // UPPER_PRDLST_CODE. 축종 코드 (PK 일부)
  String upperPrdlstCode,

  // BRD_CO. 사육 두수 (DPL/M2M의 BRD_HAD_CO와 비교할 원천값)
  Long brdCo,

  // MGR_CODE. 축종 운영상태 (휴·폐업 시 두수=0이 정상)
  String mgrCode,

  // EXAMIN_MTH_CODE. 조사방법 코드 (자동/수기 조사 출처 신뢰도)
  String examinMthCode,

  // EXAMIN_DE. 조사 일자
  LocalDateTime examinDe,

  // UPDT_DE. 수정 일자 (방역본부 수정 polling 기준)
  LocalDateTime updtDe
) {

}
