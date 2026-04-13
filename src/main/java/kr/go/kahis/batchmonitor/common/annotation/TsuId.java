package kr.go.kahis.batchmonitor.common.annotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;

import io.hypersistence.tsid.TSID;
import io.hypersistence.tsid.TSID.Factory;
import io.hypersistence.utils.hibernate.id.TsidValueGenerator;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Supplier;
import kr.go.kahis.batchmonitor.common.generator.TsuIdGenerator;
import org.hibernate.annotations.IdGeneratorType;
import org.hibernate.annotations.ValueGenerationType;

@IdGeneratorType(TsuIdGenerator.class)
@ValueGenerationType(generatedBy = TsidValueGenerator.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({FIELD, METHOD})
public @interface TsuId {

  Class<? extends Supplier<Factory>> value() default FactorySupplier.class;

  /**
   * Prefix 문자
   *
   * @return string
   * @apiNote Prefix 문자<br>
   *          ID 유형이 String 인 경우에만 적용됨
   * @author 류성재
   */
  String prefix() default "";

  /**
   * 구분 문자
   *
   * @return string
   * @apiNote 구분 문자<br>
   *          ID 유형이 String 인 경우에만 적용됨
   * @author 류성재
   */
  String separate() default "-";

  /**
   * 생성 시점의 날짜 시간을 ID 에 포함할지 여부
   *
   * @return boolean
   * @apiNote 생성 시점의 날짜 시간을 ID 에 포함할지 여부<br>
   *          milliseconds 단위로 변환되어 ID에 포함됨<br>
   *          ID 유형이 String 인 경우에만 적용됨
   * @author 류성재
   */
  boolean useEpoch() default true;

  class FactorySupplier implements Supplier<TSID.Factory> {

    public static final FactorySupplier INSTANCE = new FactorySupplier();

    private final TSID.Factory tsidFactory = TSID.Factory.builder()
        .withRandomFunction(TSID.Factory.THREAD_LOCAL_RANDOM_FUNCTION)
        .build();

    @Override
    public TSID.Factory get() {
      return tsidFactory;
    }
  }

}
