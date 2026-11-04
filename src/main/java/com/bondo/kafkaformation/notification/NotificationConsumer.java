package com.bondo.kafkaformation.notification;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consumer group "notification-service" : reçoit AUSSI tous les messages du topic,
 * indépendamment du groupe "stock-service" (chaque groupe a ses propres offsets).
 *
 * Il échoue volontairement :
 *  - produit "poison"   -> RuntimeException : 3 retries puis DLT
 *  - JSON illisible     -> IllegalArgumentException : DLT immédiat (non retryable)
 */
@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final JsonMapper jsonMapper;

    public NotificationConsumer(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = "${formation.topics.orders}", groupId = "notification-service")
    public void onOrder(ConsumerRecord<String, String> record) {
        JsonNode order;
        try {
            order = jsonMapper.readTree(record.value());
        } catch (Exception e) {
            throw new IllegalArgumentException("Message illisible : " + record.value(), e);
        }

        if ("poison".equalsIgnoreCase(order.path("product").asString())) {
            throw new RuntimeException("Service d'email indisponible pour la commande " + order.path("orderId").asString());
        }

        log.info("[NOTIF] partition={} offset={} -> email envoyé au client {}",
                record.partition(), record.offset(), record.key());
    }
}
