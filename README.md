# kafka-formation

Projet d'auto-formation Kafka avec Spring Boot 4.1 / Java 21 : un service de commandes publie des événements, consommés par un service stock et un service notification, avec retries et dead letter topic.

```
POST /orders ──► [orders] (3 partitions, clé = customerId)
                    ├──► groupe stock-service         -> réserve le stock
                    └──► groupe notification-service  -> envoie l'email
                              │ échec (3 retries)
                              ▼
                        [orders-dlt] ──► groupe dlt-monitor
```

## Lancer

1. `docker compose up -d` (Kafka sur `localhost:9092`, UI sur http://localhost:8080)
2. Ouvrir le dossier dans IntelliJ (Maven est intégré), lancer `KafkaFormationApplication`
   ou en ligne de commande : `mvn spring-boot:run`
3. Envoyer une commande :
   ```
   curl -X POST "http://localhost:8085/orders?customerId=c1&product=clavier&quantity=2"
   ```
   Sous PowerShell, utiliser `curl.exe` (et non `curl`, qui est un alias d'Invoke-WebRequest).

## Exercices (étape 1 et 2 du plan)

**1. Clé et partitions**
Envoie 5 commandes pour `c1`, puis 5 pour `c2`. Toutes celles d'un même client tombent sur la même partition.
Pourquoi c'est la seule façon de garantir l'ordre ?

**2. Deux groupes indépendants**
Chaque message est lu par STOCK et par NOTIF. Dans l'UI, onglet *Consumers* : chaque groupe a ses propres offsets.

**3. Rebalancing**
Lance une 2e instance : `mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8082`
Observe dans les logs et l'UI la redistribution des 3 partitions. Lances-en une 3e puis une 4e : que fait la 4e ?

**4. Poison pill et DLT**
```
curl -X POST "http://localhost:8085/orders?customerId=c1&product=poison"
```
Observe : 3 retries espacés d'1 s côté NOTIF, puis le message arrive dans `orders-dlt` avec ses headers d'origine.
STOCK, lui, l'a traité normalement. Question d'entretien : pendant les retries, que deviennent les autres messages de la même partition ?

**5. Lag**
Ajoute un `Thread.sleep(2000)` dans `StockConsumer`, envoie `POST /orders/bulk?count=50`, regarde le lag du groupe `stock-service` dans l'UI.

**6. Relecture**
Arrête l'appli, change le `groupId` de `StockConsumer` (`stock-service-v2`), relance : tout l'historique est relu (`auto-offset-reset: earliest`).
Pourquoi ? Que se passerait-il avec `latest` ?

## Prochaines étapes

- Remplacer les String JSON par Avro + Schema Registry
- Tests d'intégration avec Testcontainers (Kafka)
- Passer à 3 brokers, `min.insync.replicas=2`, et tuer un broker pendant un envoi
