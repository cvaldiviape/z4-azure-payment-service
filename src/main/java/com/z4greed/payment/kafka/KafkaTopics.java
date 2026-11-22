package com.z4greed.payment.kafka;

// Nombres de los topics que usa Payment Service. Centralizarlos evita literales repetidos y errores de tipeo.
public final class KafkaTopics {
    public static final String PAYMENT_PROCESSING_REQUESTS = "payment-processing-requests-topic";
    public static final String PAYMENT_PROCESSING_RESULTS = "payment-processing-results-topic";

    private KafkaTopics() {
    }
}
