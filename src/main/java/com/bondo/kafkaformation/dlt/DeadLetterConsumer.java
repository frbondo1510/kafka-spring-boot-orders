package com.bondo.kafkaformation.dlt;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Surveille le DLT. En vrai projet : alerte, stockage pour analyse, ou rejeu manuel après correction.
 * Le recoverer ajoute des headers utiles : topic/partition/offset d'origine, exception, stacktrace.
 */
@Component
public class DeadLetterConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    @KafkaListener(topics = "${formation.topics.orders-dlt}", groupId = "dlt-monitor")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        log.error("[DLT] message en échec : origine={}-{}@{} cause={} valeur={}",
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                header(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                header(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                record.value());
    }

    /** Les en-têtes partition/offset sont des entiers binaires, les autres du texte. */
    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        if (h == null) {
            return "?";
        }
        byte[] v = h.value();
        if (v.length == 4) {
            return String.valueOf(java.nio.ByteBuffer.wrap(v).getInt());
        }
        if (v.length == 8) {
            return String.valueOf(java.nio.ByteBuffer.wrap(v).getLong());
        }
        return new String(v, StandardCharsets.UTF_8);
    }
}
