package com.z4greed.payment.kafka.producer;

import com.z4greed.payment.entity.OutboxEventEntity;
import com.z4greed.payment.exception.CustomRetryableKafkaException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PaymentEventProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final long sendTimeoutMilliseconds;

  public PaymentEventProducer(
      KafkaTemplate<String, String> kafkaTemplate,
      @Value("${app.outbox.send-timeout-milliseconds}") long sendTimeoutMilliseconds) {
    this.kafkaTemplate = kafkaTemplate;
    this.sendTimeoutMilliseconds = sendTimeoutMilliseconds;
  }

  public void publishAndWait(OutboxEventEntity event) {
    try {
      var result = this.kafkaTemplate.send(event.getTopic(), event.getEventKey(), event.getPayload())
          .get(this.sendTimeoutMilliseconds, TimeUnit.MILLISECONDS);
      log.info("action=event_published topic={} partition={} offset={} eventType={} eventId={} correlationId={} orderId={}",
          event.getTopic(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset(),
          event.getEventType(), event.getEventId(), event.getCorrelationId(), event.getAggregateId());
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new CustomRetryableKafkaException("Kafka publication was interrupted", exception);
    } catch (Exception exception) {
      throw new CustomRetryableKafkaException("Kafka did not confirm the outbox event", exception);
    }
  }

}
