package com.z4greed.payment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.payment.entity.PaymentAttemptEntity;
import com.z4greed.payment.entity.PaymentEntity;
import com.z4greed.payment.entity.ProcessedEventEntity;
import com.z4greed.payment.enums.ErrorCodeEnum;
import com.z4greed.payment.enums.EventTypeEnum;
import com.z4greed.payment.exception.GreedException;
import com.z4greed.payment.kafka.event.EventEnvelopeDto;
import com.z4greed.payment.kafka.producer.PaymentEventProducer;
import com.z4greed.payment.repository.PaymentAttemptRepository;
import com.z4greed.payment.repository.PaymentRepository;
import com.z4greed.payment.repository.ProcessedEventRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class PaymentService {
  private static final String APPROVED_TOKEN = "TEST_APPROVED";

  private final PaymentRepository paymentRepository;
  private final PaymentAttemptRepository paymentAttemptRepository;
  private final ProcessedEventRepository processedEventRepository;
  private final PaymentEventProducer paymentEventProducer;
  private final ObjectMapper objectMapper;

  public PaymentService(
      PaymentRepository paymentRepository,
      PaymentAttemptRepository paymentAttemptRepository,
      ProcessedEventRepository processedEventRepository,
      PaymentEventProducer paymentEventProducer,
      ObjectMapper objectMapper) {
    this.paymentRepository = paymentRepository;
    this.paymentAttemptRepository = paymentAttemptRepository;
    this.processedEventRepository = processedEventRepository;
    this.paymentEventProducer = paymentEventProducer;
    this.objectMapper = objectMapper;
  }

  public void process(String rawEvent) {
    EventEnvelopeDto eventEnvelope = readEvent(rawEvent);
    boolean isPaymentRequest = EventTypeEnum.PAYMENT_REQUESTED.getValue().equals(eventEnvelope.eventType());
    boolean eventWasProcessed = processedEventRepository.existsById(eventEnvelope.eventId());
    if (!isPaymentRequest || eventWasProcessed) {
      return;
    }

    Long orderId = Long.valueOf(eventEnvelope.aggregateId());
    boolean paymentAlreadyExists = paymentRepository.findByOrderId(orderId).isPresent();
    if (paymentAlreadyExists) {
      this.processedEventRepository.save(new ProcessedEventEntity(eventEnvelope));
      return;
    }

    String paymentToken = eventEnvelope.payload().get("paymentToken").asText();
    Long customerId = eventEnvelope.payload().get("customerId").asLong();
    BigDecimal amount = eventEnvelope.payload().get("amount").decimalValue();
    String currency = eventEnvelope.payload().get("currency").asText();
    boolean approved = APPROVED_TOKEN.equals(paymentToken);

    PaymentEntity paymentEntity =
        PaymentEntity.process(orderId, customerId, amount, currency, approved);
    this.paymentRepository.save(paymentEntity);
    PaymentAttemptEntity paymentAttemptEntity =
        PaymentAttemptEntity.builder().payment(paymentEntity).build();
    this.paymentAttemptRepository.save(paymentAttemptEntity);

    String eventType = approved ? EventTypeEnum.PAYMENT_APPROVED.getValue() : EventTypeEnum.PAYMENT_FAILED.getValue();
    publishEvent(eventEnvelope, eventType, paymentEntity);
    this.processedEventRepository.save(new ProcessedEventEntity(eventEnvelope));
  }

  private void publishEvent(
      EventEnvelopeDto sourceEvent, String eventType, PaymentEntity paymentEntity) {
    String eventId = UUID.randomUUID().toString();
    Map<String, Object> mapPayload = Map.of("paymentId", paymentEntity.getPaymentId());
    EventEnvelopeDto eventEnvelopeDto =
        EventEnvelopeDto.builder()
            .eventId(eventId)
            .eventType(eventType)
            .eventVersion(1)
            .aggregateId(sourceEvent.aggregateId())
            .correlationId(sourceEvent.correlationId())
            .causationId(sourceEvent.eventId())
            .timestamp(Instant.now())
            .producer("payment-service")
            .payload(objectMapper.valueToTree(mapPayload))
            .build();
    this.paymentEventProducer.publish(eventEnvelopeDto);
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return objectMapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }
}
