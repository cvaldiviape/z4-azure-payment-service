package com.z4greed.payment.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ErrorCodeEnum {
  INVALID_EVENT("Invalid event"),
  OUTBOX_SERIALIZATION_FAILED("Outbox event serialization failed");
  private final String message;
}
