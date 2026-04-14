package kr.go.kahis.batchmonitor.parser;

import java.util.Map;
import kr.go.kahis.batchmonitor.persistence.enumeration.ErrorType;

public record ParsedError(
    ErrorType errorType,
    String rawMessage,
    Map<String, String> metadata
) {

  public ParsedError {
    metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    rawMessage = rawMessage == null ? "" : rawMessage;
    errorType = errorType == null ? ErrorType.UNKNOWN : errorType;
  }
}
