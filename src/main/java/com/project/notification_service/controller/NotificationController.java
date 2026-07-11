package com.project.notification_service.controller;

import com.project.notification_service.entity.Notification;
import com.project.notification_service.repository.NotificationRepository;
import com.project.notification_service.service.NotificationProducer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import org.slf4j.MDC;
import java.util.UUID;

@RestController
@RequestMapping("/notifications")
public class NotificationController {

    private final NotificationRepository repository;
    private final KafkaTemplate<String, Notification> kafkaTemplate;
    private final NotificationProducer notificationProducer;

    public NotificationController(NotificationRepository repository, KafkaTemplate<String, Notification> kafkaTemplate,
            NotificationProducer notificationProducer
    ) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.notificationProducer = notificationProducer;
    }

    @PostMapping
    public String sendNotification(@RequestBody Notification notification) {
        // Save to PostgreSQL Database
        Notification savedNotification = repository.save(notification);

        // Generate FedEx Tracking Number
        String traceId = UUID.randomUUID().toString();
        MDC.put("traceId", traceId);
        System.out.println("Received API Request to send notification"); // Will log with traceId!

        // Publish to Kafka Topic
        kafkaTemplate.send(notificationProducer.buildKafkaMessage(savedNotification, traceId));

        MDC.clear(); // Clear the MDC after the request is processed
        return "Notification processed and published successfully!";
    }
}
