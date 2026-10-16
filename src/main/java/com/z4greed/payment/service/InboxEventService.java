package com.z4greed.payment.service;

import com.z4greed.payment.kafka.event.IntegrationEventDto;

public interface InboxEventService {
  boolean wasAlreadyProcessed(String eventId);
  void register(IntegrationEventDto event, String sourceTopic);
}
