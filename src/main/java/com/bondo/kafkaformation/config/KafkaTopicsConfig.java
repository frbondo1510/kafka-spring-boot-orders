package com.bondo.kafkaformation.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Les beans NewTopic sont créés au démarrage par le KafkaAdmin auto-configuré
 * (s'ils n'existent pas déjà).
 *
 * Le DLT a volontairement le même nombre de partitions que le topic principal :
 * le recoverer republie un message en échec sur la MÊME partition.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic ordersTopic(@Value("${formation.topics.orders}") String name,
                         @Value("${formation.partitions}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic ordersDltTopic(@Value("${formation.topics.orders-dlt}") String name,
                            @Value("${formation.partitions}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
    }
}
