package com.bondo.kafkaformation.order;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;
    private final String topic;

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate,
                         JsonMapper jsonMapper,
                         @Value("${formation.topics.orders}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
        this.topic = topic;
    }

    /**
     * L'envoi est ASYNCHRONE : send() retourne immédiatement.
     * La confirmation (ou l'erreur) du broker arrive dans le callback.
     */
    public CompletableFuture<SendResult<String, String>> send(OrderEvent order) {
        String payload = jsonMapper.writeValueAsString(order);

        return kafkaTemplate.send(topic, order.customerId(), payload)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Échec d'envoi de la commande {}", order.orderId(), ex);
                    } else {
                        var meta = result.getRecordMetadata();
                        log.info("Commande {} (client {}) -> partition {}, offset {}",
                                order.orderId(), order.customerId(), meta.partition(), meta.offset());
                    }
                });
    }
}
