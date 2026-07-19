package com.project.notification_service.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.project.notification_service.entity.Notification;
import com.project.notification_service.enums.NotificationStatus;
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
        String traceId = UUID.randomUUID().toString();

        try {
            MDC.put("traceId", traceId);

            Notification savedNotification = repository.save(notification);
            MDC.put("notificationId", String.valueOf(savedNotification.getId()));
            MDC.put("status", "ACCEPTED");
            logger.info("Received API Request to send notification");

            if (!preferenceService.checkPreference(savedNotification.getUserId(), savedNotification.getType())) {
                savedNotification.setStatus(NotificationStatus.SKIPPED.getStatus());
                repository.save(savedNotification);
                MDC.put("status", "SKIPPED");
                logger.info("Notification skipped: user has disabled {} notifications.", savedNotification.getType());
                return "Notification skipped: user has disabled " + savedNotification.getType() + " notifications.";
            }

            kafkaTemplate.send(notificationProducer.buildKafkaMessage(savedNotification, traceId));
            MDC.put("status", "PUBLISHED");
            logger.info("Notification processed and published successfully!");
            return "Notification processed and published successfully!";
        } finally {
            MDC.clear();
            logger.info("MDC cleared in NotificationController");
        }
    }
}
