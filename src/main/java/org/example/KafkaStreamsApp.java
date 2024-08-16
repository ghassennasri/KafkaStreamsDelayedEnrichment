package org.example;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Properties;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.apache.kafka.streams.state.Stores.timestampedKeyValueStoreBuilder;

public class KafkaStreamsApp {
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        //adding this will create a state store named "my-aggregate-store"
        /*builder.addStateStore(
                timestampedKeyValueStoreBuilder(persistentKeyValueStore("my-aggregate-store"), Serdes.String(), Serdes.Integer()));*/
        // This would also create a state store named "my-aggregate-store". If both are used, it will throw an exception stating that the store already exists
        Materialized<String, Integer, KeyValueStore<Bytes, byte[]>> stringLegacyKeyValueStoreMaterialized = Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(
                        "my-aggregate-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer());

        KGroupedStream<String, Integer> groupedStream = builder
                .stream("input-topic", Consumed.with(Serdes.String(), Serdes.Integer()))
                .groupByKey();

        KTable<String, Integer> aggregatedTable = groupedStream.aggregate(
                () -> 0, // Initializer should return an Integer
                (key, value, aggregate) -> (Integer)aggregate + value, // sums the values
                stringLegacyKeyValueStoreMaterialized
        );


        //Reuse the state store within a process method
        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Integer()))
                .process(() -> new Processor<String, Integer>() {
                    private KeyValueStore<String, ValueAndTimestamp<Integer>> stateStore;

                    @Override
                    public void init(ProcessorContext context) {
                        this.stateStore = (KeyValueStore<String, ValueAndTimestamp<Integer>>) context.getStateStore("my-aggregate-store");
                    }

                    @Override
                    public void process(String key, Integer value) {
                        ValueAndTimestamp<Integer> aggregateValue = stateStore.get(key);
                        if (aggregateValue.value() != null) {
                            // Do something with the value -- logging for now
                            System.out.println("Processed key: " + key + ", Aggregate Value: " + aggregateValue);
                        }
                    }

                    @Override
                    public void close() {
                    }
                }, "my-aggregate-store");

        return builder.build();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("application.id", "kafka-streams-example");
        props.put("bootstrap.servers", "localhost:9092");

        KafkaStreams streams = new KafkaStreams(new KafkaStreamsApp().createTopology(), props);
        streams.start();
    }
}
