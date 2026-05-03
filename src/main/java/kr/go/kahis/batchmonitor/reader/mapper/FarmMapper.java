package kr.go.kahis.batchmonitor.reader.mapper;

import java.util.List;
import kr.go.kahis.batchmonitor.reader.dto.FarmIdDplDto;
import kr.go.kahis.batchmonitor.reader.dto.FarmIdLsfarmDto;
import kr.go.kahis.batchmonitor.reader.dto.FarmInfoDto;
import kr.go.kahis.batchmonitor.reader.dto.FarmScaleDetailDto;
import kr.go.kahis.batchmonitor.reader.dto.FarmScaleDto;
import kr.go.kahis.batchmonitor.reader.dto.MobileBreedingLivestockHistoryDto;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface FarmMapper {

  // 1. 사육두수 히스토리 조회
  List<MobileBreedingLivestockHistoryDto> selectMobileBreedingLivestockHistory(String farmId,
      String speciesCode);

  // 2. dpl + m2m 에서 farm id 조회
  FarmIdDplDto selectFarmIdDpl(String farmId);

  // 3. lsfarm에서 farm id 조회
  FarmIdLsfarmDto selectFarmIdLsfarm(String farmId);

  // 4. lsfarm에서 farm info 조회
  FarmInfoDto selectFarmInfo(String farmId);

  // 5. lsfarm에서 farm scale 조회
  List<FarmScaleDto> selectFarmScale(String farmId);

  // 6. lsfarm에서 farm scale detail 조회
  List<FarmScaleDetailDto> selectFarmScaleDetail(String farmId);
}
