package com.z4greed.payment.exception;

import com.z4greed.payment.enums.ErrorCodeEnum;
import lombok.Getter;

@Getter
public class CustomNonRetryableKafkaException extends RuntimeException {
  private final ErrorCodeEnum errorCode;

  public CustomNonRetryableKafkaException(ErrorCodeEnum errorCode, Throwable cause) {
    super(errorCode.getMessage(), cause);
    this.errorCode = errorCode;
  }
}
