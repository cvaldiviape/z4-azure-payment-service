package com.z4greed.payment.kafka.factory;

import com.z4greed.payment.enums.EventTypeEnum;
import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.shared.kafka.event.IntegrationEventFactory;
import org.springframework.stereotype.Component;

// Adapta la fábrica genérica de z4-shared-kafka a los tipos de evento de Payment Service.
@Component
public class PaymentEventFactory {
    private final IntegrationEventFactory integrationEventFactory;

    public PaymentEventFactory(IntegrationEventFactory integrationEventFactory) {
        this.integrationEventFactory = integrationEventFactory;
    }

    public IntegrationEventDto build(EventTypeEnum eventTypeEnum, IntegrationEventDto previousEvent, Object payload) {
        String eventType = eventTypeEnum.getValue();

        return this.integrationEventFactory.create(eventType, previousEvent, payload);
    }
}
