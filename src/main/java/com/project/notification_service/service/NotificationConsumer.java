package com.project.notification_service.service;

import com.project.notification_service.entity.Notification;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {

    @KafkaListener(topics = "notification-events", groupId = "notification-group")
    public void consume(Notification notification) {
        System.out.println("------------------------------------------------");
        System.out.println("Received message from Kafka belt!");
        System.out.println("Sending email to User: " + notification.getUserId());
        System.out.println("Content: " + notification.getMessage());
        System.out.println("------------------------------------------------");
    }
}
