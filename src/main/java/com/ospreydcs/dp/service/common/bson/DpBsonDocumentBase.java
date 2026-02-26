package com.ospreydcs.dp.service.common.bson;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public abstract class DpBsonDocumentBase {

    // instance variables
    private List<String> tags;
    private Map<String, String> attributes;
    private Instant createdAt;
    private Instant updatedAt;

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    // NOTE - this method uses "add" instead of "set" to avoid problems in the Mongo framework expecting
    // methods that start with "set" to have a corresponding property.
    public void addCreationTime() {
        setCreatedAt(Instant.now());
    }

    // NOTE - this method uses "add" instead of "set" to avoid problems in the Mongo framework expecting
    // methods that start with "set" to have a corresponding property.
    public void addUpdatedTime() {
        setUpdatedAt(Instant.now());
    }
}
