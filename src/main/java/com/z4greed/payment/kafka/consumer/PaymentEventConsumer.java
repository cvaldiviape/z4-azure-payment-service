package com.z4greed.payment.kafka.consumer;

import com.z4greed.payment.service.payment.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {
  private final PaymentService paymentService;

  public PaymentEventConsumer(PaymentService paymentService) {
    this.paymentService = paymentService;
  }

  // Permanece a la escucha de los eventos de "payments-events-topic" y solo procesa PAYMENT_REQUESTED.
  @KafkaListener(topics = "payments-events-topic")
  public void consumePaymentEvents(String rawEvent) {
    this.paymentService.process(rawEvent);
  }

}
