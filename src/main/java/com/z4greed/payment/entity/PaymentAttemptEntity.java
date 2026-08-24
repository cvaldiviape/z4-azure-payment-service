package com.z4greed.payment.entity;

import jakarta.persistence.*;
import java.time.Instant;
import lombok.*;

@Entity
@Table(name = "payment_attempts")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PaymentAttemptEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private String paymentId;
  private int attemptNumber;
  private String result;
  private String errorMessage;
  private Instant createdAt;

  @Builder
  public PaymentAttemptEntity(PaymentEntity payment) {
    this.paymentId = payment.getPaymentId();
    this.attemptNumber = 1;
    this.result = payment.getStatus().name();
    this.errorMessage = payment.getFailureReason();
    this.createdAt = Instant.now();
  }
}
