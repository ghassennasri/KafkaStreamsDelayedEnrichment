package org.example;

import java.util.Objects;

public class EnrichedEvent implements KafkaEvent {
    private String id;
    private String userId;
    private double amount;
    private String category; // Enriched field

    // Default constructor
    public EnrichedEvent() {}

    public EnrichedEvent(Event event, Enrichment enrichment) {
        this.id = event.getId();
        this.userId = event.getUserId();
        this.amount = event.getAmount();
        this.category = enrichment.getCategory();
    }

    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EnrichedEvent)) return false;
        EnrichedEvent that = (EnrichedEvent) o;
        return Double.compare(that.amount, amount) == 0 &&
                Objects.equals(id, that.id) &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(category, that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, userId, amount, category);
    }

    @Override
    public String toString() {
        return "EnrichedEvent{id='" + id + "', userId='" + userId + "', amount=" + amount +
                ", category='" + category + "'}";
    }
}
