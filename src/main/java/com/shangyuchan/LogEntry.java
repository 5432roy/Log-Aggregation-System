package com.shangyuchan;

import com.google.gson.Gson;
import java.time.Instant;
import java.util.UUID;

public class LogEntry {
    private String id;
    private String timestamp;
    private String service;
    private String level;
    private String message;

    // Constructor: generate a new unique id and current timestamp.
    public LogEntry(String service, String level, String message) {
        this.id = UUID.randomUUID().toString();
        this.timestamp = Instant.now().toString();
        this.service = service;
        this.level = level;
        this.message = message;
    }

    // Constructor to allow overriding the id and timestamp (useful for simulation)
    public LogEntry(String id, String timestamp, String service, String level, String message) {
        this.id = id;
        this.timestamp = timestamp;
        this.service = service;
        this.level = level;
        this.message = message;
    }

    public String getId() { return id; }
    public String getTimestamp() { return timestamp; }
    public String getService() { return service; }
    public String getLevel() { return level; }
    public String getMessage() { return message; }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
