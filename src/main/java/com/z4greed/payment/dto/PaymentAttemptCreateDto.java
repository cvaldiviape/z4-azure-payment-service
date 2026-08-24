package com.z4greed.payment.dto;

import java.time.LocalDateTime;
import lombok.Builder;

@Builder
public record PaymentAttemptCreateDto(
    String paymentId,
    Integer attemptNumber,
    String result,
    String errorMessage,
    LocalDateTime createdAt) {}
