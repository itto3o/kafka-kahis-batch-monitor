package kr.go.kahis.batchmonitor.common.config;

import feign.auth.BasicAuthRequestInterceptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

public class AirflowClientConfig {

  @Bean
  public BasicAuthRequestInterceptor airflowBasicAuth(
      @Value("${airflow.api.username}") String username,
      @Value("${airflow.api.password}") String password) {
    return new BasicAuthRequestInterceptor(username, password);
  }
}