package com.z4greed.payment.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum EventTypeEnum {
  PAYMENT_REQUESTED("PAYMENT_REQUESTED"),
  PAYMENT_APPROVED("PAYMENT_APPROVED"),
  PAYMENT_FAILED("PAYMENT_FAILED");

  private final String value;
}
