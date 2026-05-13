package kr.go.kahis.batchmonitor.domain.parser.data;

public record LivestockErrorData(
    String farmNumber,
    String speciesCode,
    String currentValue,
    String previousValue
) {

}
