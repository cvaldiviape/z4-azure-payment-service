package com.z4greed.payment.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.payment.enums.ErrorCodeEnum;
import com.z4greed.payment.exception.GreedException;
import com.z4greed.payment.kafka.event.EventEnvelopeDto;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;

  public PaymentEventProducer(
      KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
  }

  public void publish(EventEnvelopeDto eventEnvelopeDto) {
    try {
      String eventJson = this.objectMapper.writeValueAsString(eventEnvelopeDto);
      this.kafkaTemplate.send("payments-events-topic", eventEnvelopeDto.aggregateId(), eventJson);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.EVENT_PUBLISH_FAILED, exception);
    }
  }

}