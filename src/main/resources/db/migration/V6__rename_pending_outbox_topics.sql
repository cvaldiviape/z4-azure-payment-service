UPDATE outbox_events
SET topic = 'payment-processing-results-topic'
WHERE status = 'PENDING'
  AND topic = 'payments-events-topic';
