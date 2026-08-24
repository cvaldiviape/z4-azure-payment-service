package com.z4greed.payment.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.z4greed.payment.dto.PaymentAttemptCreateDto;
import com.z4greed.payment.dto.PaymentCreateDto;
import com.z4greed.payment.entity.PaymentAttemptEntity;
import com.z4greed.payment.entity.PaymentEntity;
import com.z4greed.payment.entity.ProcessedEventEntity;
import com.z4greed.payment.enums.*;
import com.z4greed.payment.exception.GreedException;
import com.z4greed.payment.kafka.event.EventEnvelopeDto;
import com.z4greed.payment.kafka.producer.PaymentEventProducer;
import com.z4greed.payment.mapper.*;
import com.z4greed.payment.repository.*;
import com.z4greed.payment.service.PaymentService;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class PaymentServiceImpl implements PaymentService {
  private static final String APPROVED_TOKEN = "TEST_APPROVED";
  private static final String FAILURE_REASON = "Simulated payment rejection";

  private final PaymentRepository paymentRepository;
  private final PaymentAttemptRepository paymentAttemptRepository;
  private final ProcessedEventRepository processedEventRepository;
  private final PaymentEventProducer paymentEventProducer;
  private final PaymentMapper paymentMapper;
  private final PaymentAttemptMapper paymentAttemptMapper;
  private final ProcessedEventMapper processedEventMapper;
  private final ObjectMapper objectMapper;

  public PaymentServiceImpl(
      PaymentRepository paymentRepository,
      PaymentAttemptRepository paymentAttemptRepository,
      ProcessedEventRepository processedEventRepository,
      PaymentEventProducer paymentEventProducer,
      PaymentMapper paymentMapper,
      PaymentAttemptMapper paymentAttemptMapper,
      ProcessedEventMapper processedEventMapper,
      ObjectMapper objectMapper) {
    this.paymentRepository = paymentRepository;
    this.paymentAttemptRepository = paymentAttemptRepository;
    this.processedEventRepository = processedEventRepository;
    this.paymentEventProducer = paymentEventProducer;
    this.paymentMapper = paymentMapper;
    this.paymentAttemptMapper = paymentAttemptMapper;
    this.processedEventMapper = processedEventMapper;
    this.objectMapper = objectMapper;
  }

  @Override
  public void process(String rawEvent) {
    EventEnvelopeDto eventEnvelopeDto = this.readEvent(rawEvent);
    if (this.shouldIgnore(eventEnvelopeDto)) {
      return;
    }
    if (this.paymentExists(eventEnvelopeDto)) {
      this.markAsProcessed(eventEnvelopeDto);
      return;
    }

    PaymentEntity paymentEntity = this.createPayment(eventEnvelopeDto);
    this.createPaymentAttempt(paymentEntity);
    this.publishPaymentResult(eventEnvelopeDto, paymentEntity);
    this.markAsProcessed(eventEnvelopeDto);
  }

  private EventEnvelopeDto readEvent(String rawEvent) {
    try {
      return this.objectMapper.readValue(rawEvent, EventEnvelopeDto.class);
    } catch (Exception exception) {
      throw new GreedException(ErrorCodeEnum.INVALID_EVENT, exception);
    }
  }

  private Boolean shouldIgnore(EventEnvelopeDto eventEnvelopeDto) {
    Boolean isPaymentRequest = EventTypeEnum.PAYMENT_REQUESTED.getValue().equals(eventEnvelopeDto.eventType());
    Boolean wasProcessed = this.processedEventRepository.existsById(eventEnvelopeDto.eventId());
    return !isPaymentRequest || wasProcessed;
  }

  private Boolean paymentExists(EventEnvelopeDto eventEnvelopeDto) {
    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());
    return this.paymentRepository.findByOrderId(orderId).isPresent();
  }

  private PaymentEntity createPayment(EventEnvelopeDto eventEnvelopeDto) {
    PaymentCreateDto paymentCreateDto = this.createPaymentDto(eventEnvelopeDto);
    PaymentEntity paymentEntity = this.paymentMapper.toEntity(paymentCreateDto);
    return this.paymentRepository.save(paymentEntity);
  }

  private PaymentCreateDto createPaymentDto(EventEnvelopeDto eventEnvelopeDto) {
    String paymentToken = eventEnvelopeDto.payload().get("paymentToken").asText();
    Boolean approved = APPROVED_TOKEN.equals(paymentToken);
    PaymentStatusEnum status = approved ? PaymentStatusEnum.APPROVED : PaymentStatusEnum.FAILED;
    String failureReason = approved ? null : FAILURE_REASON;
    Long orderId = Long.valueOf(eventEnvelopeDto.aggregateId());
    Long customerId = eventEnvelopeDto.payload().get("customerId").asLong();
    BigDecimal amount = eventEnvelopeDto.payload().get("amount").decimalValue();
    String currency = eventEnvelopeDto.payload().get("currency").asText();

    return PaymentCreateDto.builder()
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
    PaymentAttemptCreateDto paymentAttemptCreateDto = PaymentAttemptCreateDto.builder()
        .paymentId(paymentEntity.getPaymentId())
        .attemptNumber(1)
        .result(paymentEntity.getStatus().name())
        .errorMessage(paymentEntity.getFailureReason())
        .createdAt(LocalDateTime.now())
        .build();
    PaymentAttemptEntity paymentAttemptEntity = this.paymentAttemptMapper.toEntity(paymentAttemptCreateDto);
    this.paymentAttemptRepository.save(paymentAttemptEntity);
  }

  private void publishPaymentResult(EventEnvelopeDto sourceEvent, PaymentEntity paymentEntity) {
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
        .payload(this.objectMapper.valueToTree(mapPayload))
        .build();
    this.paymentEventProducer.publish(eventEnvelopeDto);
  }

  private void markAsProcessed(EventEnvelopeDto eventEnvelopeDto) {
    ProcessedEventEntity processedEventEntity = this.processedEventMapper.toEntity(eventEnvelopeDto);
    this.processedEventRepository.save(processedEventEntity);
  }

}
