package kr.go.kahis.batchmonitor.messaging.consumer;

import kr.go.kahis.batchmonitor.messaging.dto.KafkaEvent;
import org.springframework.kafka.support.Acknowledgment;

public interface KafkaConsumer {

  void consume(KafkaEvent event, Acknowledgment acknowledgment);
}
