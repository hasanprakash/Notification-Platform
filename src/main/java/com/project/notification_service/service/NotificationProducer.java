package com.project.notification_service.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import com.project.notification_service.entity.Notification;

@Service
public class NotificationProducer {
    private static final String TOPIC = "notification-events";
    private static final Logger logger = LoggerFactory.getLogger(NotificationProducer.class);

    public Message<Notification> buildKafkaMessage(Notification notification, String traceId) {
        logger.info("Publishing to Kafka Topic: {} with Trace ID: {}", TOPIC, traceId);
        return MessageBuilder
            .withPayload(notification)
            .setHeader(KafkaHeaders.TOPIC, TOPIC)
            .setHeader("trace_id", traceId) // Sneak it in the header!
            .build();
    }
}
