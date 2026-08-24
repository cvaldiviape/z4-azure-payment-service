package com.z4greed.payment.entity;

import com.z4greed.payment.enums.PaymentStatusEnum;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "payments")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PaymentEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private String paymentId;
  private Long orderId;
  private Long customerId;
  private BigDecimal amount;
  private String currency;

  @Enumerated(EnumType.STRING)
  private PaymentStatusEnum status;

  private String failureReason;
  private Instant createdAt;
  private Instant updatedAt;

  public static PaymentEntity process(
      Long orderId, Long customerId, BigDecimal amount, String currency, boolean approved) {
    PaymentEntity paymentEntity = new PaymentEntity();
    paymentEntity.paymentId = UUID.randomUUID().toString();
    paymentEntity.orderId = orderId;
    paymentEntity.customerId = customerId;
    paymentEntity.amount = amount;
    paymentEntity.currency = currency;
    paymentEntity.status = approved ? PaymentStatusEnum.APPROVED : PaymentStatusEnum.FAILED;
    paymentEntity.failureReason = approved ? null : "Simulated payment rejection";
    paymentEntity.createdAt = Instant.now();
    return paymentEntity;
  }
}
