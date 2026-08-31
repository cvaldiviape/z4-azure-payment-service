package com.z4greed.payment.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ErrorCodeEnum {
  INVALID_EVENT("Invalid event"),
  EVENT_PUBLISH_FAILED("Event publish failed");
  private final String message;
}
