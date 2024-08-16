package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;

import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaStreamsAppTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Integer> inputTopic;
    private TestOutputTopic<String, Integer> outputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());

        KafkaStreamsApp app = new KafkaStreamsApp();
        testDriver = new TopologyTestDriver(app.createTopology(), props);

        inputTopic = testDriver.createInputTopic("input-topic", Serdes.String().serializer(), Serdes.Integer().serializer());
        outputTopic = testDriver.createOutputTopic("output-topic", Serdes.String().deserializer(), Serdes.Integer().deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void testAggregationAndProcessing() {
        inputTopic.pipeInput("key1", 1);
        inputTopic.pipeInput("key1", 2);
        inputTopic.pipeInput("key2", 3);


    }
    @Test
    void testAggregationResult() {
        inputTopic.pipeInput("key1", 1);
        inputTopic.pipeInput("key1", 2);
        inputTopic.pipeInput("key1", 3);

        // Testing the final state of the aggregation
        ValueAndTimestamp<Object> result = testDriver.getTimestampedKeyValueStore("my-aggregate-store").get("key1");
        assertEquals(6, result.value());  // 1 + 2 + 3 = 6
    }

    @Test
    void testStateStoreAccessInProcessor() {
        inputTopic.pipeInput("key1", 4);
        inputTopic.pipeInput("key1", 5);

        ValueAndTimestamp<Object> result = testDriver.getTimestampedKeyValueStore("my-aggregate-store").get("key1");
        assertEquals(9, result.value()); // 4 + 5 = 9

        // Process a new record and check the state store again
        inputTopic.pipeInput("key1", 6);
        result = testDriver.getTimestampedKeyValueStore("my-aggregate-store").get("key1");
        assertEquals(15, result.value()); // 9 + 6 = 15
    }
  /*  @Test
    void testProcessorStateException() {
        // Injecting a failure after closing the driver, trying to provoke the ProcessorStateException
        inputTopic.pipeInput("key1", 1);
        inputTopic.pipeInput("key1", 2);


        assertThrows(org.apache.kafka.streams.errors.ProcessorStateException.class, () -> {

            testDriver.close();

            // Attempt to forward or process after the driver is closed
            inputTopic.pipeInput("key1", 3);
        });
    }*/
}
