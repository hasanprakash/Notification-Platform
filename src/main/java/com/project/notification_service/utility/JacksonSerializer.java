package com.project.notification_service.utility;

import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;
import java.util.Map;

public class JacksonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JacksonSerializer() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            if (data == null) return null;
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() { }
}