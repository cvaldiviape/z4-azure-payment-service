package com.z4greed.payment.service;

import com.z4greed.payment.entity.OutboxEventEntity;
import com.z4greed.payment.kafka.event.IntegrationEventDto;
import java.util.List;

public interface OutboxEventService {
  void enqueue(String topic, IntegrationEventDto event);
  List<OutboxEventEntity> findPending(int batchSize);
  void markAsPublished(String eventId);
  void registerFailedAttempt(String eventId, String errorMessage);
}
