package com.z4greed.payment.dto;

import com.z4greed.payment.enums.PaymentStatusEnum;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.Builder;

@Builder
public record PaymentCreateDto(
    String paymentId,
    Long orderId,
    Long customerId,
    BigDecimal amount,
    String currency,
    PaymentStatusEnum status,
    String failureReason,
    Instant createdAt) {}
