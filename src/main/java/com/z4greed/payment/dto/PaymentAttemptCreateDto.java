package com.z4greed.payment.dto;

import java.time.Instant;
import lombok.Builder;

@Builder
public record PaymentAttemptCreateDto(
    String paymentId,
    int attemptNumber,
    String result,
    String errorMessage,
    Instant createdAt) {}
