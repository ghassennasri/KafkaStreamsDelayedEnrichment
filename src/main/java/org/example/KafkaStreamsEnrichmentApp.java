package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class KafkaStreamsEnrichmentApp {

    public static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Create a persistent state store for pending events that need enrichment keyed by ID
        StoreBuilder<KeyValueStore<String, Event>> pendingEventsStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("pending-events-store"),
                Serdes.String(),
                new EventSerde()
        );
        builder.addStateStore(pendingEventsStore);

        // Create a persistent state store for enrichment records keyed by user ID
        StoreBuilder<KeyValueStore<String, Enrichment>> enrichmentStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("enrichment-store"),
                Serdes.String(),
                new EnrichmentSerde()
        );
        builder.addStateStore(enrichmentStore);

        // Process events: try immediate enrichment by looking up in enrichment-store
        builder.stream("event-topic", Consumed.with(Serdes.String(), new EventSerde()))
                .selectKey((key, event) -> event.getUserId())// repartition by user ID
                .transform(() -> new EventEnrichmentTransformer(),Named.as("event-enrichment-transformer"), "enrichment-store", "pending-events-store")
                .to("enriched-events-topic", Produced.with(Serdes.String(), new EnrichedEventSerde()));

        // Process late enrichment: update the enrichment-store and check for pending events
        builder.stream("enrichment-topic", Consumed.with(Serdes.String(), new EnrichmentSerde()))
                .process(() -> new EnrichmentUpdateProcessor(), Named.as("late-enrichment-processor"), "enrichment-store", "pending-events-store");

        return builder.build().addSink("late-enrichment-sink", "enriched-events-topic", Serdes.String().serializer(), new EnrichedEventSerde().serializer(),"late-enrichment-processor");
    }


    // Transformer for event-topic: performs immediate enrichment when possible, otherwise stores the event for later processing
    static class EventEnrichmentTransformer implements Transformer<String, Event, KeyValue<String, EnrichedEvent>> {
        private KeyValueStore<String, Enrichment> enrichmentStore;
        private KeyValueStore<String, Event> pendingEvents;

        @Override
        public void init(ProcessorContext context) {
            enrichmentStore = (KeyValueStore<String, Enrichment>) context.getStateStore("enrichment-store");
            pendingEvents = (KeyValueStore<String, Event>) context.getStateStore("pending-events-store");
            context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                try (KeyValueIterator<String, Event> iter = pendingEvents.all()) {
                    while (iter.hasNext()) {
                        KeyValue<String, Event> entry = iter.next();
                        String enrichmentKey = entry.key;
                        Event pendingEvent = entry.value;
                        // Check if the enrichment for this key is now available
                        Enrichment enrichment = enrichmentStore.get(enrichmentKey);
                        if (enrichment != null) {
                            // Forward enriched event
                            // usse the original event’s identifier as the output key.
                            context.forward(pendingEvent.getId(), new EnrichedEvent(pendingEvent, enrichment));
                            // Remove the event from pending store once processed
                            pendingEvents.delete(enrichmentKey);
                        }
                    }
                }
            });
        }

        @Override
        public KeyValue<String, EnrichedEvent> transform(String key, Event event) {
            // Use event's userId to lookup for the enrichment matching the user ID
            String userId = event.getUserId();
            Enrichment enrichment = enrichmentStore.get(userId);
            if (enrichment != null) {
                // Enrichment found: forward enriched event using the original event key (order ID)
                return KeyValue.pair(event.getId(), new EnrichedEvent(event, enrichment));
            } else {
                // Enrichment not found: store the event keyed by userId for later processing
                pendingEvents.put(userId, event);
                return null;
            }
        }

        @Override
        public void close() {}
    }

    // Processor for enrichment-topic: updates enrichment store and processes late-arriving enrichments for pending events
    static class EnrichmentUpdateProcessor implements Processor<String, Enrichment> {
        private KeyValueStore<String, Enrichment> enrichmentStore;
        private KeyValueStore<String, Event> pendingEvents;
        private ProcessorContext context;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            enrichmentStore = (KeyValueStore<String, Enrichment>) context.getStateStore("enrichment-store");
            pendingEvents = (KeyValueStore<String, Event>) context.getStateStore("pending-events-store");
        }

        @Override
        public void process(String key, Enrichment enrichment) {
            // The enrichment key here is the user ID
            enrichmentStore.put(key, enrichment);

            // Check if there's a pending event for this user
            Event pendingEvent = pendingEvents.get(key);
            if (pendingEvent != null) {
                // Forward enriched event using the original event key (order ID)
                context.forward(pendingEvent.getId(), new EnrichedEvent(pendingEvent, enrichment));
                pendingEvents.delete(key);
            }
        }

        @Override
        public void close() {}
    }


    /* Could be also a valid solution solution to use stream-stream join and specify valid window  */
    public static Topology buildTopologyWithWindowedJoin() {
        StreamsBuilder builder = new StreamsBuilder();

        // Stream events from event-topic and repartition by user ID.
        KStream<String, Event> eventStream = builder
                .stream("event-topic", Consumed.with(Serdes.String(), new EventSerde()))
                .selectKey((key, event) -> event.getUserId());

        // Stream enrichments from enrichment-topic and repartition by user ID.
        KStream<String, Enrichment> enrichmentStream = builder
                .stream("enrichment-topic", Consumed.with(Serdes.String(), new EnrichmentSerde()))
                .selectKey((key, enrichment) -> enrichment.getUserId());

        //1-minute window join with 1-hour grace period
        KStream<String, EnrichedEvent> enrichedStream = eventStream.join(
                enrichmentStream,
                (event, enrichment) -> new EnrichedEvent(event, enrichment),
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofHours(1)),
                //could use deprecated JoinWindows.of(Duration.ofMinutes(1)) which includes a grace period of 24 hours
                StreamJoined.with(Serdes.String(), new EventSerde(), new EnrichmentSerde())
        );

        // Write the result to the output topic
        enrichedStream.to("enriched-events-topic", Produced.with(Serdes.String(), new EnrichedEventSerde()));

        return builder.build();
    }


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-enrichment-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde.class.getName());

        KafkaStreams streams = new KafkaStreams(createTopology(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
    // JSON Serde implementation for generic JSON serialization/deserialization.
    public static class JsonSerde<T extends KafkaEvent> implements Serde<T> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final Class<T> type;

        public JsonSerde(Class<T> type) {
            this.type = type;
        }

        @Override
        public Serializer<T> serializer() {
            return new JsonSerializer<>();
        }

        @Override
        public Deserializer<T> deserializer() {
            JsonDeserializer<T> deserializer = new JsonDeserializer<>();
            return deserializer;
        }

        @Override
        public void close() {}
    }

    // Specific Serde implementations.
    public static class EventSerde extends JsonSerde<Event> {
        public EventSerde() {
            super(Event.class);
        }
    }

    public static class EnrichmentSerde implements Serde<Enrichment> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Serializer<Enrichment> serializer() {
            return (topic, data) -> {
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new RuntimeException("Error serializing Enrichment object", e);
                }
            };
        }

        @Override
        public Deserializer<Enrichment> deserializer() {
            return (topic, data) -> {
                try {
                    if (data == null) {
                        return null;
                    }
                    return objectMapper.readValue(data, Enrichment.class);
                } catch (IOException e) {
                    throw new RuntimeException("Error deserializing Enrichment object", e);
                }
            };
        }

        @Override
        public void close() {}
    }

    public static class EnrichedEventSerde extends JsonSerde<EnrichedEvent> {
        public EnrichedEventSerde() {
            super(EnrichedEvent.class);
        }
    }
    public static class JsonSerializer<T> implements Serializer<T> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, T data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing JSON message", e);
            }
        }
    }

    // Concrete Deserializer Implementation
    public static class JsonDeserializer<T> implements Deserializer<T> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public T deserialize(String topic, byte[] data) {
            try {
                return (T) objectMapper.readValue(data, KafkaEvent.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing JSON message", e);
            }
        }
    }
}
