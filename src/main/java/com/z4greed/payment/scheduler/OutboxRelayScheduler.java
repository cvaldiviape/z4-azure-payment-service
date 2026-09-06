package com.z4greed.payment.scheduler;

import com.z4greed.payment.entity.OutboxEventEntity;
import com.z4greed.payment.kafka.producer.PaymentEventProducer;
import com.z4greed.payment.service.OutboxEventService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OutboxRelayScheduler {
  private final OutboxEventService outboxService;
  private final PaymentEventProducer producer;
  private final int batchSize;

  public OutboxRelayScheduler(OutboxEventService outboxService, PaymentEventProducer producer,
      @Value("${app.outbox.batch-size}") int batchSize) {
    this.outboxService = outboxService;
    this.producer = producer;
    this.batchSize = batchSize;
  }

  @Scheduled(fixedDelayString = "${app.outbox.poll-interval-milliseconds}",
      initialDelayString = "${app.outbox.initial-delay-milliseconds}")
  public void publishPendingEvents() {
    List<OutboxEventEntity> events = this.outboxService.findPending(this.batchSize);
    events.forEach(this::publishPendingEvent);
  }

  private void publishPendingEvent(OutboxEventEntity event) {
    try {
      this.producer.publishAndWait(event);
      this.outboxService.markAsPublished(event.getEventId());
    } catch (RuntimeException exception) {
      String message = exception.getMessage() == null ? "Unknown error" : exception.getMessage();
      this.outboxService.registerFailedAttempt(event.getEventId(), message.substring(0, Math.min(2000, message.length())));
      log.error("action=outbox_publish_failed eventType={} eventId={} correlationId={} orderId={} attempts={}",
          event.getEventType(), event.getEventId(), event.getCorrelationId(), event.getAggregateId(),
          event.getAttempts() + 1, exception);
    }
  }

}