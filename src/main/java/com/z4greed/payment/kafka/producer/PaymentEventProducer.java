package com.z4greed.payment.kafka.producer;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.payment.enums.ErrorCodeEnum;
import com.z4greed.payment.exception.GreedException;
import com.z4greed.payment.kafka.event.EventEnvelopeDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PaymentEventProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final ObjectMapper objectMapper;

  public PaymentEventProducer(
      KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
    this.kafkaTemplate = kafkaTemplate;
    this.objectMapper = objectMapper;
  }

  public void publish(String topic, EventEnvelopeDto eventEnvelopeDto) {
    try {
      String eventJson = this.objectMapper.writeValueAsString(eventEnvelopeDto);
      this.kafkaTemplate.send(topic, eventEnvelopeDto.aggregateId(), eventJson).whenComplete((sendResult, exception) -> {
        if (exception != null) {
          log.error("action=event_publish_failed topic={} eventType={} eventId={} correlationId={} orderId={}", topic, eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), exception);
          return;
        }

        log.info("action=event_published topic={} partition={} offset={} eventType={} eventId={} correlationId={} orderId={}", topic, sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset(), eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      });
    } catch (Exception exception) {
      log.error("action=event_serialization_failed topic={} eventType={} eventId={} correlationId={} orderId={}", topic, eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), exception);
      throw new GreedException(ErrorCodeEnum.EVENT_PUBLISH_FAILED, exception);
    }
  }

}
