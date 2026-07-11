package com.project.notification_service.service;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import com.project.notification_service.entity.Notification;

@Service
public class NotificationProducer {
    private static final String TOPIC = "notification-events";

    public Message<Notification> buildKafkaMessage(Notification notification, String traceId) {
        System.out.println("Publishing to Kafka Topic: " + TOPIC + " with Trace ID: " + traceId);
        return MessageBuilder
            .withPayload(notification)
            .setHeader(KafkaHeaders.TOPIC, TOPIC)
            .setHeader("trace_id", traceId) // Sneak it in the header!
            .build();
    }
}
