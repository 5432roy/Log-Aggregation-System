package com.shangyuchan;

import com.google.gson.Gson;
import java.time.Instant;

public class LogEntry {
    private String timestamp;
    private String service;
    private String level;
    private String message;

    public LogEntry(String service, String level, String message) {
        timestamp = Instant.now().toString();
        this.service = service;
        this.level = level;
        this.message = message;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
