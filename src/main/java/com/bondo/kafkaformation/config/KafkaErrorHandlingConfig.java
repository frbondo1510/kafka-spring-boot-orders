package com.bondo.kafkaformation.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Gestion des erreurs de TOUS les @KafkaListener de l'application.
 * Spring Boot branche automatiquement ce bean CommonErrorHandler sur la container factory.
 *
 * Comportement :
 *  1. le listener lève une exception
 *  2. retry 3 fois, à 1 seconde d'intervalle (même message, même partition : la partition est bloquée pendant ce temps)
 *  3. toujours en échec -> le message est publié dans "<topic>-dlt" et l'offset est commité
 *  4. la consommation reprend au message suivant
 *
 * Les exceptions "non retryables" (ex. message illisible) partent directement en DLT :
 * réessayer un message mal formé ne sert à rien.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + "-dlt", record.partition()));

        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));

        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("Échec n°{} sur {}-{}@{} : {}", attempt,
                        record.topic(), record.partition(), record.offset(), ex.getMessage()));
        return handler;
    }
}
