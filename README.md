# Payment service

Servicio responsable de simular el procesamiento de pagos de la Saga de compra.

## Configuración local

| Propiedad | Valor predeterminado |
|---|---|
| Puerto HTTP | `8084` |
| PostgreSQL | `localhost:5436/payments_db` |
| Kafka | `localhost:9092` |
| Consumer group ID | `payment-event-consumer-group-id` |

## Kafka

`PaymentEventConsumer` permanece a la escucha de:

```text
payments-events-topic
```

El topic contiene solicitudes y resultados, pero este servicio solamente procesa:

```text
PAYMENT_REQUESTED
```

Después publica en el mismo topic uno de estos resultados:

```text
PAYMENT_APPROVED
PAYMENT_FAILED
```

El token `TEST_APPROVED` simula una aprobación. Cualquier otro valor simula un rechazo.

## Variables disponibles

```text
PAYMENTS_DB_URL
PAYMENTS_DB_USERNAME
PAYMENTS_DB_PASSWORD
KAFKA_BOOTSTRAP_SERVERS
SPRING_PROFILES_ACTIVE
```

## Ejecutar y depurar

Desde la raíz del proyecto:

```bash
docker compose -f infra/docker-compose.yml up -d kafka payments-postgres
```

Después ejecuta `PaymentServiceApplication` con **Debug** en IntelliJ.

Para compilar sin ejecutar pruebas:

```bash
./mvnw clean package -DskipTests
```
