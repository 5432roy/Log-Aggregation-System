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
    
    public String getService() {
        return this.service;
    }

    public String getLevel() {
        return this.level;
    }

    public String getMessage() {
        return this.message;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
