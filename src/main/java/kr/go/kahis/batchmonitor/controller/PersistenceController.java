package kr.go.kahis.batchmonitor.controller;

import kr.go.kahis.batchmonitor.dto.request.ErrorRequest;
import kr.go.kahis.batchmonitor.service.PersistenceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class PersistenceController {

  private final PersistenceService service;

  // airflow error -> spring controller -> kafka publish -> kafka consumer -> spring (reader service) -> airflow clear
  @PostMapping("/api/v1/errors")
  public ResponseEntity<?> publishError(@RequestBody ErrorRequest request) {
    service.publish(request);
    return ResponseEntity.accepted().build();
  }
}
