package org.example;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonDeserializer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers

public class KafkaStreamsEnrichmentAppIntegrationTest {
    @Container
    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    private KafkaStreams streams;

    private static final int NUM_PARTITIONS = 3; // Number of partitions for testing

    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        // Create topics with multiple partitions
        createTopics();

        // Start the Kafka Streams application
        Properties props = new Properties();
        props.put("application.id", "kafka-enrichment-app-test");
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("default.key.serde", Serdes.String().getClass().getName());
        props.put("default.value.serde", KafkaStreamsEnrichmentApp.EventSerde.class.getName());

        streams = new KafkaStreams(KafkaStreamsEnrichmentApp.createTopology(), props);
        streams.start();
    }

    @AfterEach
    public void tearDown() {
        if (streams != null) {
            streams.close();
        }
    }

    @Test
    public void testCoPartitioningWithMultiplePartitions() throws Exception {
        // Produce events and enrichments for multiple users across partitions
        try (KafkaProducer<String, Event> eventProducer = createProducer();
             KafkaProducer<String, Enrichment> enrichmentProducer = createEnrichmentProducer()) {

            // User 1: Event first, then enrichment
            eventProducer.send(new ProducerRecord<>("event-topic", "order1", new Event("order1", "user1",150.0))).get();
            Thread.sleep( 2000);
            enrichmentProducer.send(new ProducerRecord<>("enrichment-topic", "user1", new Enrichment("user1", "Gold"))).get();

            // User 2: Enrichment first, then event
            enrichmentProducer.send(new ProducerRecord<>("enrichment-topic", "user2", new Enrichment("user2", "platinium"))).get();
            eventProducer.send(new ProducerRecord<>("event-topic", "order2", new Event("order2", "user2",30))).get();

            // User 3: Event only (no enrichment)
            eventProducer.send(new ProducerRecord<>("event-topic", "order3", new Event("order3", "user3",250))).get();
        }

        // Consume enriched events from the enriched-events-topic
        try (KafkaConsumer<String, EnrichedEvent> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList("enriched-events-topic"));
            List<ConsumerRecord<String, EnrichedEvent>> records = new ArrayList<>();


            // Poll for records until we have at least 2 records or timeout
            long startTime = System.currentTimeMillis();
            while (records.size() < 2 && (System.currentTimeMillis() - startTime) < 30_000) { // 30-second timeout
                ConsumerRecords<String, EnrichedEvent> polledRecords = consumer.poll(Duration.ofSeconds(10));

                // Add all records from the poll to the list
                polledRecords.forEach(record -> {
                    System.out.printf("Consumed record: key=%s, value=%s, partition=%d, offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                    records.add(record);
                });
            }
            // Validate enriched events
            assertEquals(2, records.size());
            records.forEach(rec -> {
                if ("order1".equals(rec.key())) {
                    assertEquals("Gold", rec.value().getCategory());
                } else if ("order2".equals(rec.key())) {
                    assertEquals("platinium", rec.value().getCategory());
                }
            });
        }
    }

    private void createTopics() throws ExecutionException, InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic("event-topic", NUM_PARTITIONS, (short) 1),
                    new NewTopic("enrichment-topic", NUM_PARTITIONS, (short) 1),
                    new NewTopic("enriched-events-topic", NUM_PARTITIONS, (short) 1)
            );
            adminClient.createTopics(topics).all().get();
        }
    }

    private KafkaProducer<String, Event> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaStreamsEnrichmentApp.JsonSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private KafkaProducer<String, Enrichment> createEnrichmentProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaStreamsEnrichmentApp.JsonSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, EnrichedEvent> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("group.id", "test-group");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", KafkaStreamsEnrichmentApp.JsonDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

}
