package kr.go.kahis.batchmonitor.vo;

import kr.go.kahis.batchmonitor.domain.kahis.mapper.FarmMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class LsFarmIdFinder {

  private final FarmMapper mapper;

  public String find(String farmId) {
    String dplFarmId = mapper.selectFarmIdDpl(farmId).frmhsNo();

    return mapper.selectFarmIdLsfarm(dplFarmId).cntcFrmhsNo();
  }
}
