package com.z4greed.payment.service;

import com.z4greed.payment.entity.OutboxEventEntity;
import com.z4greed.payment.kafka.event.EventEnvelopeDto;
import java.util.List;

public interface OutboxEventService {
  void enqueue(String topic, EventEnvelopeDto eventEnvelopeDto);
  List<OutboxEventEntity> findPending(int batchSize);
  void markAsPublished(String eventId);
  void registerFailedAttempt(String eventId, String errorMessage);
}