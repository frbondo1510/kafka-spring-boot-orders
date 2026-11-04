package com.bondo.kafkaformation.order;

import java.time.Instant;

/**
 * Événement publié dans le topic "orders".
 * Clé Kafka = customerId : toutes les commandes d'un même client vont dans la même partition,
 * donc elles sont consommées dans l'ordre.
 */
public record OrderEvent(String orderId, String customerId, String product, int quantity, Instant createdAt) {
}
