package com.project.notification_service.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.project.notification_service.entity.Notification;
import com.project.notification_service.repository.NotificationRepository;
import com.project.notification_service.service.NotificationProducer;
import com.project.notification_service.service.PreferenceService;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import org.slf4j.MDC;
import java.util.UUID;

@RestController
@RequestMapping("/notifications")
public class NotificationController {

    private static final Logger logger = LoggerFactory.getLogger(NotificationController.class);

    private final NotificationRepository repository;
    private final KafkaTemplate<String, Notification> kafkaTemplate;
    private final NotificationProducer notificationProducer;
    private final PreferenceService preferenceService;

    public NotificationController(NotificationRepository repository, KafkaTemplate<String, Notification> kafkaTemplate,
            NotificationProducer notificationProducer, PreferenceService preferenceService
    ) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.notificationProducer = notificationProducer;
        this.preferenceService = preferenceService;
    }

    @PostMapping
    public String sendNotification(@RequestBody Notification notification) {
        // Save to PostgreSQL Database
        Notification savedNotification = repository.save(notification);

        // Generate FedEx Tracking Number
        String traceId = UUID.randomUUID().toString();
        MDC.put("traceId", traceId);
        logger.info("Received API Request to send notification, traceId: {}", traceId); 

        if (!preferenceService.checkPreference(savedNotification.getUserId(), savedNotification.getType())) {
            savedNotification.setStatus("SKIPPED");
            repository.save(savedNotification);
            MDC.clear();
            logger.info("Notification skipped: user has disabled {} notifications.", savedNotification.getType());
            return "Notification skipped: user has disabled " + savedNotification.getType() + " notifications.";
        }

        // Publish to Kafka Topic
        kafkaTemplate.send(notificationProducer.buildKafkaMessage(savedNotification, traceId));

        MDC.clear(); // Clear the MDC after the request is processed
        logger.info("Notification processed and published successfully!");
        return "Notification processed and published successfully!";
    }
}
