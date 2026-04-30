package kr.go.kahis.batchmonitor.reader.dto;

import java.time.LocalDateTime;

public record MobileBreedingLivestockHistoryDto(
  // FRMSH_NO. 농장 번호
  String frmhsNo,

  // LSTKSP_CL. 축종 분류
  String lstkspCl,

  // BRD_HAD_CO. 사육 두수
  Long brdHadCo,

  // LAST_CHANGE_DT. 마지막 변경 일시
  LocalDateTime lastChangeDt
) {

}
