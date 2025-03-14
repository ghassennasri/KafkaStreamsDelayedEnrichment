package org.example;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

// Enable polymorphic handling using an interface
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Event.class, name = "event"),
        @JsonSubTypes.Type(value = EnrichedEvent.class, name = "enrichedEvent")
})
public interface KafkaEvent {
    String getId();
}
