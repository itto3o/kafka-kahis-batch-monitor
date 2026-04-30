package kr.go.kahis.batchmonitor.common.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

/**
 * 유닛 서비스
 *
 * @author 류성재
 * @apiNote 논리적인 Architecture 를 설계하기 위한 Annotation<br>
 *          Service 와 똑같은 기능
 */
@Target({TYPE})
@Retention(RUNTIME)
@Documented
@Component
public @interface Unit {

  /**
   * Value
   *
   * @return string string
   * @apiNote Service 에 있는 value 와 똑같은 기능
   * @author 류성재
   */
  @AliasFor(annotation = Component.class)
  String value() default "";

}
