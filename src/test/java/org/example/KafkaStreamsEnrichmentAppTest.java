package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.*;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaStreamsEnrichmentAppTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsEnrichmentAppTest.class);

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Event> eventInputTopic;
    private TestInputTopic<String, Enrichment> enrichmentInputTopic;
    private TestOutputTopic<String, EnrichedEvent> outputTopic;

    @BeforeEach
    public void setup() {
        Topology topology = KafkaStreamsEnrichmentApp.createTopology();
        //print the topology
        System.out.println(topology.describe());
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-enrichment-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaStreamsEnrichmentApp.EventSerde.class.getName());

        testDriver = new TopologyTestDriver(topology, props);

        eventInputTopic = testDriver.createInputTopic("event-topic", Serdes.String().serializer(), new KafkaStreamsEnrichmentApp.EventSerde().serializer());
        enrichmentInputTopic = testDriver.createInputTopic("enrichment-topic", Serdes.String().serializer(), new KafkaStreamsEnrichmentApp.EnrichmentSerde().serializer());
        outputTopic = testDriver.createOutputTopic("enriched-events-topic", Serdes.String().deserializer(), new KafkaStreamsEnrichmentApp.EnrichedEventSerde().deserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testImmediateEnrichment() {
        Instant now = Instant.now();
        long eventTimestamp = now.toEpochMilli();
        long enrichmentTimestamp = now.minusSeconds(5).toEpochMilli(); // Enrichment arrives earlier than event

        Event event = new Event("order123", "user1", 150.75);
        Enrichment enrichment = new Enrichment("user1", "Gold Member");
        LOGGER.info("Sending Enrichment first (timestamp: {}) -> {}", enrichmentTimestamp, enrichment);
        enrichmentInputTopic.pipeInput("user1", enrichment, enrichmentTimestamp);
        LOGGER.info("Sending Event after (timestamp: {}) -> {}", eventTimestamp, event);
        eventInputTopic.pipeInput("order123", event, eventTimestamp);
        KeyValue<String, EnrichedEvent> result = outputTopic.readKeyValue();
        assertNotNull(result);
        // Validate order
        LOGGER.info("Received EnrichedEvent (timestamp: {}) -> {}", eventTimestamp, result.value);
        assertEquals("order123", result.key);
        assertEquals("Gold Member", result.value.getCategory());
    }

    @Test
    public void testLateEnrichment() {
        Instant now = Instant.now();
        long eventTimestamp = now.toEpochMilli();
        long enrichmentTimestamp = now.plusSeconds(5).toEpochMilli(); // Enrichment arrives later than event

        Event event = new Event("order456", "user2", 99.99);

        LOGGER.info("Sending Event first (timestamp: {}) -> {}", eventTimestamp, event);
        eventInputTopic.pipeInput("order456", event, eventTimestamp);

        assertTrue(outputTopic.isEmpty(), "Output should be empty as enrichment hasn't arrived");

        // enrichment arriving late
        Enrichment enrichment = new Enrichment("user2", "Silver Member");
        LOGGER.info("Sending Late Enrichment (timestamp: {}) -> {}", enrichmentTimestamp, enrichment);
        enrichmentInputTopic.pipeInput("user2", enrichment, enrichmentTimestamp);

        // Check if the pending event is processed
        KeyValue<String, EnrichedEvent> result = outputTopic.readKeyValue();
        assertNotNull(result);

        LOGGER.info("Received EnrichedEvent after late enrichment (timestamp: {}) -> {}", enrichmentTimestamp, result.value);
        assertEquals("order456", result.key);
        assertEquals("Silver Member", result.value.getCategory());
    }
}
