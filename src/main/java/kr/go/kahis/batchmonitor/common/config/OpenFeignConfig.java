package kr.go.kahis.batchmonitor.common.config;

import static java.util.concurrent.TimeUnit.SECONDS;

import feign.Logger;
import feign.Retryer.Default;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.openfeign.FeignFormatterRegistrar;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.datetime.standard.DateTimeFormatterRegistrar;

/**
 * Open Feign 설정
 *
 * @author 류성재
 * @apiNote Open Feign 설정
 */
@Configuration
@EnableFeignClients("kr.go.kahis.batchmonitor.domain.airflow")
public class OpenFeignConfig {

  /**
   * 재시도 설정
   *
   * @return default
   * @apiNote 재시도 설정
   * @author 류성재
   */
  @Bean
  public Default retryer() {
    return new Default(100, SECONDS.toMillis(3), 5);
  }

  /**
   * Datetime Formatter 설정
   *
   * @return feign formatter registrar
   * @apiNote Datetime Formatter 설정
   * @author 류성재
   */
  @Bean
  public FeignFormatterRegistrar dateTimeFormatterRegistrar() {
    return registry -> {
      DateTimeFormatterRegistrar registrar = new DateTimeFormatterRegistrar();

      registrar.setUseIsoFormat(true);
      registrar.registerFormatters(registry);
    };
  }

  /**
   * 로그 레벨 설정
   *
   * @return logger . level
   * @apiNote 로그 레벨 설정
   * @author 류성재
   */
  @Bean
  public Logger.Level loggerLevel() {
    return Logger.Level.FULL;
  }

}
