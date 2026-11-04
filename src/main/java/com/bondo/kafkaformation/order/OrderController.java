package com.bondo.kafkaformation.order;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    /**
     * Exemple : POST http://localhost:8081/orders?customerId=c1&product=clavier&quantity=2
     * Produit "poison" -> le service notification échoue systématiquement (test du DLT).
     */
    @PostMapping
    public Map<String, Object> create(@RequestParam String customerId,
                                      @RequestParam String product,
                                      @RequestParam(defaultValue = "1") int quantity) throws Exception {
        var order = new OrderEvent(UUID.randomUUID().toString(), customerId, product, quantity, Instant.now());

        // .get() rend l'appel synchrone pour pouvoir renvoyer partition/offset dans la réponse HTTP.
        // En production on évite de bloquer ainsi un thread HTTP sur chaque envoi.
        var meta = producer.send(order).get().getRecordMetadata();

        return Map.of("orderId", order.orderId(), "partition", meta.partition(), "offset", meta.offset());
    }

    /**
     * Envoie N commandes réparties sur 5 clients, pour observer la répartition par partition.
     * Exemple : POST http://localhost:8081/orders/bulk?count=50
     */
    @PostMapping("/bulk")
    public Map<String, Object> bulk(@RequestParam(defaultValue = "20") int count) {
        for (int i = 0; i < count; i++) {
            String customerId = "c" + (i % 5);
            producer.send(new OrderEvent(UUID.randomUUID().toString(), customerId, "produit-" + i, 1, Instant.now()));
        }
        return Map.of("sent", count);
    }
}
