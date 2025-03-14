package org.example;

import java.util.Objects;

public class Enrichment {
    private String userId; // User ID to match Event
    private String category; // Enrichment metadata (e.g., VIP Customer, Regular)

    // Default constructor
    public Enrichment() {}

    public Enrichment(String userId, String category) {
        this.userId = userId;
        this.category = category;
    }

    // Getters and setters
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Enrichment)) return false;
        Enrichment that = (Enrichment) o;
        return Objects.equals(userId, that.userId) &&
                Objects.equals(category, that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, category);
    }

    @Override
    public String toString() {
        return "Enrichment{userId='" + userId + "', category='" + category + "'}";
    }
}
