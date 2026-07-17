package com.project.notification_service.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.project.notification_service.entity.Notification;
import com.project.notification_service.enums.NotificationStatus;
import com.project.notification_service.repository.NotificationRepository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class RateLimitRecoveryScheduler {

    private static final Logger logger = LoggerFactory.getLogger(RateLimitRecoveryScheduler.class);

    private final NotificationRepository repository;
    private final KafkaTemplate<String, Notification> kafkaTemplate;
    private final NotificationProducer notificationProducer;

    public RateLimitRecoveryScheduler(NotificationRepository repository, KafkaTemplate<String, Notification> kafkaTemplate, NotificationProducer notificationProducer) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.notificationProducer = notificationProducer;
    }

    // Runs every 60 seconds
    @Scheduled(fixedDelay = 60000)
    @Transactional
    public void requeueRateLimitedMessages() {
        // Find messages where status is RATE_LIMITED and the send_after time has passed
        List<Notification> readyToRetry = repository.findByStatusAndSendAfterBefore(NotificationStatus.RATE_LIMITED.getStatus(), LocalDateTime.now());

        for (Notification notification : readyToRetry) {
            logger.info("Waking up rate-limited message: {}", notification.getId());
            
            // 1. Reset status so the worker will process it
            notification.setStatus(NotificationStatus.PENDING.getStatus());
            notification.setSendAfter(null);
            repository.save(notification);

            // 2. Push back to Kafka
            kafkaTemplate.send(notificationProducer.buildKafkaMessage(notification, UUID.randomUUID().toString()));
        }
    }
}