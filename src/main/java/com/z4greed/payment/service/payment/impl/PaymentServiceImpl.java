package com.z4greed.payment.service.payment.impl;

import tools.jackson.databind.ObjectMapper;
import com.z4greed.payment.entity.PaymentAttemptEntity;
import com.z4greed.payment.entity.PaymentEntity;
import com.z4greed.payment.entity.ProcessedEventEntity;
import com.z4greed.payment.enums.*;
import com.z4greed.payment.exception.GreedException;
import com.z4greed.payment.kafka.event.EventEnvelopeDto;
import com.z4greed.payment.kafka.producer.PaymentEventProducer;
import com.z4greed.payment.mapper.ProcessedEventMapper;
import com.z4greed.payment.repository.*;
import com.z4greed.payment.service.payment.PaymentService;
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
  private final ProcessedEventRepository processedEventRepository;
  private final PaymentEventProducer paymentEventProducer;
  private final ProcessedEventMapper processedEventMapper;
  private final ObjectMapper mapper;

  public PaymentServiceImpl(
      PaymentRepository paymentRepository,
      PaymentAttemptRepository paymentAttemptRepository,
      ProcessedEventRepository processedEventRepository,
      PaymentEventProducer paymentEventProducer,
      ProcessedEventMapper processedEventMapper,
      ObjectMapper mapper
  ) {
    this.paymentRepository = paymentRepository;
    this.paymentAttemptRepository = paymentAttemptRepository;
    this.processedEventRepository = processedEventRepository;
    this.paymentEventProducer = paymentEventProducer;
    this.processedEventMapper = processedEventMapper;
    this.mapper = mapper;
  }

  @Override
  public void process(String rawEvent) {
    EventEnvelopeDto eventEnvelopeDto = this.readEvent(rawEvent);
    log.info("action=event_received eventType={} eventId={} correlationId={} orderId={} producer={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), eventEnvelopeDto.producer());

    try {
      this.processEvent(eventEnvelopeDto);
    } catch (RuntimeException exception) {
      log.error("action=event_processing_failed eventType={} eventId={} correlationId={} orderId={} exceptionType={} errorMessage=\"{}\"", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId(), exception.getClass().getSimpleName(), exception.getMessage(), exception);
      throw exception;
    }
  }

  private void processEvent(EventEnvelopeDto eventEnvelopeDto) {

    if (this.shouldIgnore(eventEnvelopeDto)) {
      log.info("action=event_ignored reason=unsupported_or_already_processed eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      return;
    }

    if (this.paymentExists(eventEnvelopeDto)) {
      this.markAsProcessed(eventEnvelopeDto);
      log.info("action=event_ignored reason=payment_already_exists eventType={} eventId={} correlationId={} orderId={}", eventEnvelopeDto.eventType(), eventEnvelopeDto.eventId(), eventEnvelopeDto.correlationId(), eventEnvelopeDto.aggregateId());
      return;
    }

    PaymentEntity paymentEntity = this.createPayment(eventEnvelopeDto);
    this.createPaymentAttempt(paymentEntity);
    EventEnvelopeDto paymentResultEvent = this.publishPaymentResult(eventEnvelopeDto, paymentEntity);
    this.markAsProcessed(eventEnvelopeDto);
    this.logPaymentResult(paymentResultEvent, paymentEntity);
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.mapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      log.error("action=event_deserialization_failed message=Invalid_Kafka_event", exception);
      throw new GreedException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private void logPaymentResult(EventEnvelopeDto paymentResultEvent, PaymentEntity paymentEntity) {
    if (paymentEntity.getStatus() == PaymentStatusEnum.FAILED) {
      log.warn("action=payment_rejected eventType={} eventId={} correlationId={} orderId={} paymentId={} reason=\"{}\"", paymentResultEvent.eventType(), paymentResultEvent.eventId(), paymentResultEvent.correlationId(), paymentResultEvent.aggregateId(), paymentEntity.getPaymentId(), paymentEntity.getFailureReason());
      return;
    }

    log.info("action=payment_approved eventType={} eventId={} correlationId={} orderId={} paymentId={}", paymentResultEvent.eventType(), paymentResultEvent.eventId(), paymentResultEvent.correlationId(), paymentResultEvent.aggregateId(), paymentEntity.getPaymentId());
  }

  private Boolean shouldIgnore(EventEnvelopeDto eventEnvelopeDto) {
    boolean isPaymentRequest = EventTypeEnum.PAYMENT_REQUESTED.getValue().equals(eventEnvelopeDto.eventType());
    boolean wasProcessed = this.processedEventRepository.existsById(eventEnvelopeDto.eventId());
    return !isPaymentRequest || wasProcessed;
  }

  private Boolean paymentExists(EventEnvelopeDto eventEnvelopeDto) {
    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());
    return this.paymentRepository.findByOrderId(orderId).isPresent();
  }

  private PaymentEntity createPayment(EventEnvelopeDto eventEnvelopeDto) {
    PaymentEntity paymentEntity = this.buildPaymentEntity(eventEnvelopeDto);
    return this.paymentRepository.save(paymentEntity);
  }

  private PaymentEntity buildPaymentEntity(EventEnvelopeDto eventEnvelopeDto) {
    String paymentToken = eventEnvelopeDto.payload().get("paymentToken").asText();
    boolean approved = "TEST_APPROVED".equals(paymentToken);

    PaymentStatusEnum status = approved
            ? PaymentStatusEnum.APPROVED
            : PaymentStatusEnum.FAILED;

    String failureReason = approved
            ? null
            : "The paymentToken is invalid";

    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());
    Long customerId = eventEnvelopeDto.payload().get("customerId").asLong();
    BigDecimal amount = eventEnvelopeDto.payload().get("amount").decimalValue();
    String currency = eventEnvelopeDto.payload().get("currency").asText();

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

  private EventEnvelopeDto publishPaymentResult(EventEnvelopeDto sourceEvent, PaymentEntity paymentEntity) {
    EventTypeEnum eventType = paymentEntity.getStatus() == PaymentStatusEnum.APPROVED
        ? EventTypeEnum.PAYMENT_APPROVED
        : EventTypeEnum.PAYMENT_FAILED;

    Map<String, Object> mapPayload = Map.of("paymentId", paymentEntity.getPaymentId());

    EventEnvelopeDto eventEnvelopeDto = EventEnvelopeDto.builder()
        .eventId(UUID.randomUUID().toString())
        .eventType(eventType.getValue())
        .eventVersion(1)
        .aggregateId(sourceEvent.aggregateId())
        .correlationId(sourceEvent.correlationId())
        .causationId(sourceEvent.eventId())
        .timestamp(LocalDateTime.now())
        .producer("payment-service")
        .payload(this.mapper.valueToTree(mapPayload))
        .build();

    this.paymentEventProducer.publish("payments-events-topic", eventEnvelopeDto);
    return eventEnvelopeDto;
  }

  private void markAsProcessed(EventEnvelopeDto eventEnvelopeDto) {
    ProcessedEventEntity processedEventEntity = this.processedEventMapper.toEntity(eventEnvelopeDto);
    this.processedEventRepository.save(processedEventEntity);
  }

}
