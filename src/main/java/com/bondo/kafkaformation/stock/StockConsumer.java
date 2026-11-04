package com.bondo.kafkaformation.stock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer group "stock-service".
 * Lance une 2e instance de l'appli (autre port) : les 3 partitions sont réparties entre les instances
 * du même groupe (rebalancing). Avec 4 instances, l'une restera inactive : 3 partitions = 3 consumers max.
 */
@Component
public class StockConsumer {

    private static final Logger log = LoggerFactory.getLogger(StockConsumer.class);

    @KafkaListener(topics = "${formation.topics.orders}", groupId = "stock-service")
    public void onOrder(ConsumerRecord<String, String> record) {
        log.info("[STOCK] partition={} offset={} client={} -> réservation du stock : {}",
                record.partition(), record.offset(), record.key(), record.value());
    }
}
