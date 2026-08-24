package com.z4greed.payment.kafka.consumer;

import com.z4greed.payment.service.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {
  private final PaymentService paymentService;

  public PaymentEventConsumer(PaymentService paymentService) {
    this.paymentService = paymentService;
  }

  @KafkaListener(topics = "payments.events")
  public void consume(String rawEvent) {
    this.paymentService.process(rawEvent);
  }
}
