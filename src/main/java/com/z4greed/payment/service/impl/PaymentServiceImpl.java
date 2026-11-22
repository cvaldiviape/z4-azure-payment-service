package com.z4greed.payment.service.impl;

import com.z4greed.payment.entity.PaymentAttemptEntity;
import com.z4greed.payment.entity.PaymentEntity;
import com.z4greed.payment.enums.EventTypeEnum;
import com.z4greed.payment.enums.PaymentStatusEnum;
import com.z4greed.payment.kafka.KafkaTopics;
import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.shared.kafka.event.IntegrationEventReader;
import com.z4greed.payment.kafka.factory.PaymentEventFactory;
import com.z4greed.payment.repository.PaymentAttemptRepository;
import com.z4greed.payment.repository.PaymentRepository;
import com.z4greed.shared.kafka.inbox.InboxEventService;
import com.z4greed.shared.kafka.outbox.OutboxEventService;
import com.z4greed.payment.service.PaymentService;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;

@Service
@Transactional
public class PaymentServiceImpl implements PaymentService {
    // Pasarela simulada: solo este token se aprueba.
    private static final String APPROVED_TEST_TOKEN = "TEST_APPROVED";
    private static final String INVALID_TOKEN_REASON = "The paymentToken is invalid";
    private static final int FIRST_ATTEMPT = 1;

    private final PaymentRepository paymentRepository;
    private final PaymentAttemptRepository paymentAttemptRepository;
    private final InboxEventService inboxEventService;
    private final OutboxEventService outboxEventService;
    private final IntegrationEventReader integrationEventReader;
    private final PaymentEventFactory paymentEventFactory;

    public PaymentServiceImpl(
            PaymentRepository paymentRepository,
            PaymentAttemptRepository paymentAttemptRepository,
            InboxEventService inboxEventService,
            OutboxEventService outboxEventService,
            IntegrationEventReader integrationEventReader,
            PaymentEventFactory paymentEventFactory
    ) {
        this.paymentRepository = paymentRepository;
        this.paymentAttemptRepository = paymentAttemptRepository;
        this.inboxEventService = inboxEventService;
        this.outboxEventService = outboxEventService;
        this.integrationEventReader = integrationEventReader;
        this.paymentEventFactory = paymentEventFactory;
    }

    @Override
    public void process(String rawEvent) {
        IntegrationEventDto event = this.integrationEventReader.read(rawEvent);

        if (!this.isPendingPaymentRequest(event)) {
            return;
        }
        if (!this.paymentExists(event)) {
            PaymentEntity paymentEntity = this.charge(event);
            this.publishPaymentResult(event, paymentEntity);
        }
        this.inboxEventService.register(event, KafkaTopics.PAYMENT_PROCESSING_REQUESTS);
    }

    private boolean isPendingPaymentRequest(IntegrationEventDto event) {
        boolean isPaymentRequest = EventTypeEnum.PAYMENT_REQUESTED.getValue().equals(event.eventType());
        return isPaymentRequest && !this.inboxEventService.wasAlreadyProcessed(event.eventId());
    }

    // Un pedido se cobra una sola vez, aunque llegue otro PAYMENT_REQUESTED con distinto eventId.
    private boolean paymentExists(IntegrationEventDto event) {
        return this.paymentRepository.findByOrderId(Long.valueOf(event.aggregateId())).isPresent();
    }

    private PaymentEntity charge(IntegrationEventDto event) {
        PaymentEntity paymentEntity = this.paymentRepository.save(this.buildPayment(event));
        this.registerAttempt(paymentEntity);
        return paymentEntity;
    }

    private PaymentEntity buildPayment(IntegrationEventDto event) {
        JsonNode payload = event.payload();
        boolean approved = APPROVED_TEST_TOKEN.equals(payload.get("paymentToken").asText());

        return PaymentEntity.builder()
                .paymentId(UUID.randomUUID().toString())
                .orderId(Long.valueOf(event.aggregateId()))
                .customerId(payload.get("customerId").asLong())
                .amount(payload.get("amount").decimalValue())
                .currency(payload.get("currency").asText())
                .status(approved ? PaymentStatusEnum.APPROVED : PaymentStatusEnum.FAILED)
                .failureReason(approved ? null : INVALID_TOKEN_REASON)
                .createdAt(LocalDateTime.now())
                .build();
    }

    private void registerAttempt(PaymentEntity paymentEntity) {
        this.paymentAttemptRepository.save(PaymentAttemptEntity.builder()
                .paymentId(paymentEntity.getPaymentId())
                .attemptNumber(FIRST_ATTEMPT)
                .result(paymentEntity.getStatus().name())
                .errorMessage(paymentEntity.getFailureReason())
                .createdAt(LocalDateTime.now())
                .build());
    }

    private void publishPaymentResult(IntegrationEventDto paymentRequestedEvent, PaymentEntity paymentEntity) {
        EventTypeEnum resultType = paymentEntity.getStatus() == PaymentStatusEnum.APPROVED
                ? EventTypeEnum.PAYMENT_APPROVED
                : EventTypeEnum.PAYMENT_FAILED;
        Map<String, Object> mapPayload = Map.of("paymentId", paymentEntity.getPaymentId());

        IntegrationEventDto resultEvent = this.paymentEventFactory.build(resultType, paymentRequestedEvent, mapPayload);
        this.outboxEventService.enqueue(KafkaTopics.PAYMENT_PROCESSING_RESULTS, resultEvent);
    }

}
