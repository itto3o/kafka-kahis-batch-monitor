package kr.go.kahis.batchmonitor.domain.parser;

import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kr.go.kahis.batchmonitor.common.enumeration.ErrorType;

public record PatternParser(
    ErrorType errorType,
    Pattern pattern,
    Function<Matcher, Map<String, String>> extractor
) {
}
