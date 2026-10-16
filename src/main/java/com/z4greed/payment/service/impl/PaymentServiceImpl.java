package com.z4greed.payment.service.impl;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.payment.entity.PaymentAttemptEntity;
import com.z4greed.payment.entity.PaymentEntity;
import com.z4greed.payment.enums.*;
import com.z4greed.payment.exception.CustomNonRetryableKafkaException;
import com.z4greed.payment.kafka.event.IntegrationEventDto;
import com.z4greed.payment.repository.*;
import com.z4greed.payment.service.InboxEventService;
import com.z4greed.payment.service.OutboxEventService;
import com.z4greed.payment.service.PaymentService;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
@Slf4j
public class PaymentServiceImpl implements PaymentService {
  private final PaymentRepository paymentRepository;
  private final PaymentAttemptRepository paymentAttemptRepository;
  private final InboxEventService inboxEventService;
  private final OutboxEventService outboxEventService;
  private final ObjectMapper mapper;

  public PaymentServiceImpl(
      PaymentRepository paymentRepository,
      PaymentAttemptRepository paymentAttemptRepository,
      InboxEventService inboxEventService,
      OutboxEventService outboxEventService,
      ObjectMapper mapper
  ) {
    this.paymentRepository = paymentRepository;
    this.paymentAttemptRepository = paymentAttemptRepository;
    this.inboxEventService = inboxEventService;
    this.outboxEventService = outboxEventService;
    this.mapper = mapper;
  }

  @Override
  public void process(String rawEvent) {
    IntegrationEventDto event = this.readEvent(rawEvent);
    log.info("action=event_received eventType={} eventId={} correlationId={} orderId={} producer={}", event.eventType(), event.eventId(), event.correlationId(), event.aggregateId(), event.producer());

    try {
      this.processEvent(event);
    } catch (RuntimeException exception) {
      log.error("action=event_processing_failed eventType={} eventId={} correlationId={} orderId={} exceptionType={} errorMessage=\"{}\"", event.eventType(), event.eventId(), event.correlationId(), event.aggregateId(), exception.getClass().getSimpleName(), exception.getMessage(), exception);
      throw exception;
    }
  }

  private IntegrationEventDto readEvent(String rawEvent) {
    try {
      return this.mapper.readValue(rawEvent, IntegrationEventDto.class);
    } catch (Exception exception) {
      log.error("action=event_deserialization_failed message=Invalid_Kafka_event", exception);
      throw new CustomNonRetryableKafkaException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private void processEvent(IntegrationEventDto event) {

    if (this.shouldIgnore(event)) {
      log.info("action=event_ignored reason=unsupported_or_already_processed eventType={} eventId={} correlationId={} orderId={}", event.eventType(), event.eventId(), event.correlationId(), event.aggregateId());
      return;
    }

    if (this.paymentExists(event)) {
      this.inboxEventService.register(event, "payment-processing-requests-topic");
      log.info("action=event_ignored reason=payment_already_exists eventType={} eventId={} correlationId={} orderId={}", event.eventType(), event.eventId(), event.correlationId(), event.aggregateId());
      return;
    }

    PaymentEntity paymentEntity = this.createPayment(event);
    this.createPaymentAttempt(paymentEntity);
    IntegrationEventDto paymentResultEvent = this.publishPaymentResult(event, paymentEntity);
    this.inboxEventService.register(event, "payment-processing-requests-topic");
    this.logPaymentResult(paymentResultEvent, paymentEntity);
  }



  private void logPaymentResult(IntegrationEventDto paymentResultEvent, PaymentEntity paymentEntity) {
    if (paymentEntity.getStatus() == PaymentStatusEnum.FAILED) {
      log.warn("action=payment_rejected eventType={} eventId={} correlationId={} orderId={} paymentId={} reason=\"{}\"", paymentResultEvent.eventType(), paymentResultEvent.eventId(), paymentResultEvent.correlationId(), paymentResultEvent.aggregateId(), paymentEntity.getPaymentId(), paymentEntity.getFailureReason());
      return;
    }

    log.info("action=payment_approved eventType={} eventId={} correlationId={} orderId={} paymentId={}", paymentResultEvent.eventType(), paymentResultEvent.eventId(), paymentResultEvent.correlationId(), paymentResultEvent.aggregateId(), paymentEntity.getPaymentId());
  }

  private Boolean shouldIgnore(IntegrationEventDto event) {
    boolean isPaymentRequest = EventTypeEnum.PAYMENT_REQUESTED.getValue().equals(event.eventType());
    boolean wasProcessed = this.inboxEventService.wasAlreadyProcessed(event.eventId());
    return !isPaymentRequest || wasProcessed;
  }

  private Boolean paymentExists(IntegrationEventDto event) {
    Long orderId = Long.valueOf(event.aggregateId());
    return this.paymentRepository.findByOrderId(orderId).isPresent();
  }

  private PaymentEntity createPayment(IntegrationEventDto event) {
    PaymentEntity paymentEntity = this.buildPaymentEntity(event);
    return this.paymentRepository.save(paymentEntity);
  }

  private PaymentEntity buildPaymentEntity(IntegrationEventDto event) {
    String paymentToken = event.payload().get("paymentToken").asText();
    boolean approved = "TEST_APPROVED".equals(paymentToken);

    PaymentStatusEnum status = approved
            ? PaymentStatusEnum.APPROVED
            : PaymentStatusEnum.FAILED;

    String failureReason = approved
            ? null
            : "The paymentToken is invalid";

    Long orderId = Long.valueOf(event.aggregateId());
    Long customerId = event.payload().get("customerId").asLong();
    BigDecimal amount = event.payload().get("amount").decimalValue();
    String currency = event.payload().get("currency").asText();

    return PaymentEntity.builder()
        .paymentId(UUID.randomUUID().toString())
        .orderId(orderId)
        .customerId(customerId)
        .amount(amount)
        .currency(currency)
        .status(status)
        .failureReason(failureReason)
        .createdAt(LocalDateTime.now())
        .build();
  }

  private void createPaymentAttempt(PaymentEntity paymentEntity) {
    PaymentAttemptEntity paymentAttemptEntity = PaymentAttemptEntity.builder()
        .paymentId(paymentEntity.getPaymentId())
        .attemptNumber(1)
        .result(paymentEntity.getStatus().name())
        .errorMessage(paymentEntity.getFailureReason())
        .createdAt(LocalDateTime.now())
        .build();

    this.paymentAttemptRepository.save(paymentAttemptEntity);
  }

  private IntegrationEventDto publishPaymentResult(IntegrationEventDto event, PaymentEntity paymentEntity) {
    EventTypeEnum eventType = paymentEntity.getStatus() == PaymentStatusEnum.APPROVED
        ? EventTypeEnum.PAYMENT_APPROVED
        : EventTypeEnum.PAYMENT_FAILED;

    Map<String, Object> mapPayload = Map.of("paymentId", paymentEntity.getPaymentId());

    IntegrationEventDto resultEvent = IntegrationEventDto.builder()
        .eventId(UUID.randomUUID().toString())
        .eventType(eventType.getValue())
        .eventVersion(1)
        .aggregateId(event.aggregateId())
        .correlationId(event.correlationId())
        .previousEventId(event.eventId())
        .timestamp(LocalDateTime.now())
        .producer("payment-service")
        .payload(this.mapper.valueToTree(mapPayload))
        .build();

    this.outboxEventService.enqueue("payment-processing-results-topic", resultEvent);
    return resultEvent;
  }

}
