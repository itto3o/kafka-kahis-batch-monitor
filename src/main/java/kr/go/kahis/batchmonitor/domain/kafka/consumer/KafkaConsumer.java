package kr.go.kahis.batchmonitor.domain.kafka.consumer;

import kr.go.kahis.batchmonitor.domain.kafka.dto.KafkaEvent;
import org.springframework.kafka.support.Acknowledgment;

public interface KafkaConsumer {

  void consume(KafkaEvent event, Acknowledgment acknowledgment);
}
