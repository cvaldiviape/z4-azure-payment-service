package com.z4greed.payment.service;

import com.z4greed.payment.kafka.event.EventEnvelopeDto;

public interface InboxEventService {
  boolean wasAlreadyProcessed(String eventId);
  void register(EventEnvelopeDto eventEnvelopeDto, String sourceTopic);
}
