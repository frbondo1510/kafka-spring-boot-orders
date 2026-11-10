package com.bondo.kafkaformation.notification;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Retries NON BLOQUANTS : un message en échec quitte la partition principale
 * et part dans un topic de retry. Le consumer passe tout de suite au message suivant.
 *
 * Parcours d'un message qui échoue toujours :
 *   orders -> orders-retry-2000 (2 s) -> orders-retry-4000 (4 s) -> orders-retry-8000 (8 s) -> orders-dlt
 *
 * Les topics de retry sont créés automatiquement au démarrage.
 * Une IllegalArgumentException (JSON illisible) part directement en DLT, sans retry.
 */
@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final JsonMapper jsonMapper;

    public NotificationConsumer(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @RetryableTopic(
            attempts = "4",                                   // 1 essai + 3 retries
            backOff = @BackOff(delay = 2000, multiplier = 2.0), // 2 s, 4 s, 8 s
            numPartitions = "3",                              // défaut = 1 : on garde 3 comme orders
            exclude = IllegalArgumentException.class,
            traversingCauses = "true")
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

        log.info("[NOTIF] topic={} partition={} offset={} -> email envoyé au client {}",
                record.topic(), record.partition(), record.offset(), record.key());
    }
}