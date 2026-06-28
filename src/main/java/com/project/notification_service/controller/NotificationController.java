package com.project.notification_service.controller;

import com.project.notification_service.entity.Notification;
import com.project.notification_service.repository.NotificationRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/notifications")
public class NotificationController {

    private final NotificationRepository repository;
    private final KafkaTemplate<String, Notification> kafkaTemplate;
    private static final String TOPIC = "notification-events";

    public NotificationController(NotificationRepository repository, KafkaTemplate<String, Notification> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }
    
    @PostMapping
    public String sendNotification(@RequestBody Notification notification) {
        // 1. Save to PostgreSQL Database
        Notification savedNotification = repository.save(notification);

        // 2. Publish to Kafka Topic
        kafkaTemplate.send(TOPIC, savedNotification);

        return "Notification processed and published successfully!";
    }
}
