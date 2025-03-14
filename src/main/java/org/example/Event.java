package org.example;

import java.util.Objects;

public class Event implements KafkaEvent {
    private String id;  // Event ID (e.g., order ID)
    private String userId; // User or entity ID to match enrichment
    private double amount; // Some associated value

    // Default constructor (required for deserialization)
    public Event() {}

    public Event(String id, String userId, double amount) {
        this.id = id;
        this.userId = userId;
        this.amount = amount;
    }

    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Event)) return false;
        Event event = (Event) o;
        return Double.compare(event.amount, amount) == 0 &&
                Objects.equals(id, event.id) &&
                Objects.equals(userId, event.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, userId, amount);
    }

    @Override
    public String toString() {
        return "Event{id='" + id + "', userId='" + userId + "', amount=" + amount + "}";
    }
}
