package kr.go.kahis.batchmonitor.parser;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kr.go.kahis.batchmonitor.persistence.enumeration.ErrorType;

public final class ParserUtil {

  private static final List<PatternParser> PATTERN_PARSERS = List.of(
      parser(ErrorType.LIVESTOCK_ANOMALY,
          "^(.+?)의\\s+(.+?)\\s+사육두수 비교에 이상이 감지되었습니다\\.\\s*당일 사육두수:\\s*([\\d.]+),\\s*전일 사육두수:\\s*([\\d.]+)$",
          matcher -> metadata(
              "farmNumber", matcher.group(1).trim(),
              "speciesCode", matcher.group(2).trim(),
              "currentValue", matcher.group(3).trim(),
              "previousValue", matcher.group(4).trim()
          )),
      parser(ErrorType.PREDICTION_ANOMALY,
          "^(.+?)의\\s+예측값이 크게 차이납니다\\.\\s*당일 예측치:\\s*([\\d.]+),\\s*전일 예측치:\\s*([\\d.]+)$",
          matcher -> metadata(
              "farmNumber", matcher.group(1).trim(),
              "currentPrediction", matcher.group(2).trim(),
              "previousPrediction", matcher.group(3).trim()
          )),
      parser(ErrorType.PNU_ANOMALY,
          "^PNU 시도 코드가 표준 코드와 다른 농장이 존재합니다\\. 확인이 필요합니다\\. PNU\\s*:\\s*(.+)$",
          matcher -> metadata("pnuCode", matcher.group(1).trim())),
      parser(ErrorType.FARM_COORDINATE_MISSING,
          "^좌표 정보가 없는 농장이 존재합니다\\.\\s*(\\d+)개\\s+농장\\s*:\\s*(.+)$",
          matcher -> metadata(
              "missingFarmCount", matcher.group(1).trim(),
              "farmList", matcher.group(2).trim()
          )),
      parser(ErrorType.DATA_NOT_FOUND,
          "^(.+?)의\\s*(데이터|방역카드 정보|농장정보|축종정보)가 존재하지 않습니다\\.$",
          matcher -> metadata(
              "standardDate", matcher.group(1).trim(),
              "resource", matcher.group(2).trim()
          )),
      parser(ErrorType.TRAININGSET_COUNT_MISMATCH,
          "^(.+?)의\\s*tb_prediction_result 데이터수가 tb_trainingset의 데이터수와 일치하지 않습니다\\.$",
          matcher -> metadata("standardDate", matcher.group(1).trim())),
      parser(ErrorType.FARM_COUNT_ANOMALY,
          "^농장 정보의 개수가 한달 평균 개수와 10% 이상 차이납니다\\. 확인이 필요합니다\\.$",
          matcher -> Map.of()),
      parser(ErrorType.FARM_COUNT_ANOMALY,
          "^전날과 농장 정보의 갯수 차이가 오차범위 이상입니다\\. 확인이 필요합니다\\.$",
          matcher -> Map.of()),
      parser(ErrorType.CALC_ENV_ANOMALY,
          "^1\\.0 초과의 ratio가 존재합니다\\. 확인이 필요합니다\\.$",
          matcher -> Map.of()),
      parser(ErrorType.CALC_ENV_ANOMALY,
          "^농가 주변환경의 비율이 모두 0인 농가가 존재합니다\\. 확인이 필요합니다\\.$",
          matcher -> Map.of()),
      parser(ErrorType.UNKNOWN,
          "^Error occurred:\\s*(.+)$",
          matcher -> metadata("exception", matcher.group(1).trim()))
  );

  private ParserUtil() {
  }

  public static ErrorType getErrorType(String message) {
    return parse(message).errorType();
  }

  public static ParsedError parse(String message) {
    if (message == null || message.isBlank()) {
      return new ParsedError(ErrorType.UNKNOWN, "", Map.of());
    }

    String normalized = message.trim();
    return PATTERN_PARSERS.stream()
        .map(patternParser -> parse(patternParser, normalized))
        .flatMap(Optional::stream)
        .findFirst()
        .orElseGet(() -> new ParsedError(ErrorType.UNKNOWN, normalized, Map.of()));
  }

  private static Optional<ParsedError> parse(PatternParser patternParser, String message) {
    Matcher matcher = patternParser.pattern().matcher(message);
    if (!matcher.matches()) {
      return Optional.empty();
    }
    return Optional.of(new ParsedError(
        patternParser.errorType(),
        message,
        patternParser.extractor().apply(matcher)
    ));
  }

  private static PatternParser parser(ErrorType errorType, String regex, Function<Matcher, Map<String, String>> extractor) {
    return new PatternParser(errorType, Pattern.compile(regex), extractor);
  }

  private static Map<String, String> metadata(String... keyValues) {
    if (keyValues == null || keyValues.length == 0) {
      return Map.of();
    }
    if (keyValues.length % 2 != 0) {
      throw new IllegalArgumentException("metadata key/value length must be even");
    }
    Map<String, String> result = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      result.put(keyValues[i], keyValues[i + 1]);
    }
    return Map.copyOf(result);
  }
}
