# Payment service

Servicio responsable de simular el procesamiento de pagos de la Saga de compra.

## Configuración local

| Propiedad | Valor predeterminado |
|---|---|
| Puerto HTTP | `8084` |
| PostgreSQL | `localhost:5436/payments_db` |
| Kafka | `localhost:9092` |
| Consumer group ID | `payment-command-consumer-group-id` |

Flyway crea y modifica el esquema mediante `db/migration`. Hibernate utiliza `ddl-auto: validate` únicamente para comprobar que las entidades coincidan con las tablas; no crea ni altera la estructura.

## Kafka

`PaymentEventConsumer` permanece a la escucha de:

```text
payments-commands-topic
```

El servicio procesa el comando:

```text
PAYMENT_REQUESTED
```

Después publica en `payments-events-topic` uno de estos resultados:

```text
PAYMENT_APPROVED
PAYMENT_FAILED
```

Payment Service consume únicamente comandos y no vuelve a recibir sus propios resultados.

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
docker compose -f infra/docker/docker-compose.yml up -d kafka payments-postgres
```

Después ejecuta `PaymentServiceApplication` con **Debug** en IntelliJ.

Para compilar sin ejecutar pruebas:

```bash
./mvnw clean package -DskipTests
```
