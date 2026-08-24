package com.z4greed.payment.entity;

import com.z4greed.payment.enums.PaymentStatusEnum;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.*;

@Entity
@Table(name = "payments")
@Getter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
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
}
