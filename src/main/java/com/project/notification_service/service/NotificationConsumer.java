package com.project.notification_service.service;

import java.util.Optional;
import java.util.Random;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

// import org.springframework.retry.annotation.Backoff;

import com.project.notification_service.entity.Notification;
import com.project.notification_service.repository.NotificationRepository;

@Service
public class NotificationConsumer {

    private final NotificationRepository repository;

    public NotificationConsumer(NotificationRepository repository) {
        this.repository = repository;
    }

    // --------------------------------------------------------
    // WORKER 1: EMAIL (With Retry & DLQ)
    // --------------------------------------------------------
    @RetryableTopic(
        attempts = "4", // 1 initial try + 3 retries
        backOff = @BackOff(delay = 2000, multiplier = 2.0), 
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        retryTopicSuffix = "-retry", 
        dltTopicSuffix = "-dlq"      
    )
    @KafkaListener(topics = "notification-events", groupId = "email-worker-group", concurrency = "3")
    public void processEmail(Notification notificationFromKafka) throws InterruptedException {
        if (!"EMAIL".equalsIgnoreCase(notificationFromKafka.getType())) {
            return; 
        }

        // 1. Fetch the absolute latest truth from Postgres using the ID!
        Notification dbNotification = repository.findById(notificationFromKafka.getId())
            .orElseThrow(() -> new RuntimeException("Notification not found in DB!"));

        // If DB says retryCount is 0, this is Attempt #1. If retryCount is 1, it's Attempt #2.
        int currentAttempt = dbNotification.getRetryCount() + 1;
        System.out.println("\n[EMAIL WORKER] Attempt #" + currentAttempt + " for User: " + dbNotification.getUserId());

        // 2. SIMULATE A RANDOM FAILURE
        boolean randomFailure = new Random().nextBoolean();
        if (randomFailure) {
            System.out.println("CRASH! Email Provider is down.");
            
            // Increment the counter in the DB and save it BEFORE throwing the exception
            dbNotification.setRetryCount(dbNotification.getRetryCount() + 1);
            repository.save(dbNotification);
            
            // Now throw the exception so Kafka moves it to the next retry topic
            throw new RuntimeException("Email Provider Failed!"); 
        }

        // 3. SUCCESS
        Thread.sleep(1000); 
        System.out.println("SUCCESS: Email sent! Message: " + dbNotification.getMessage());
        
        // (Optional) You can reset retry_count to 0 here if you want to track pure successes.
    }

    // --------------------------------------------------------
    // WORKER 2: SMS
    // --------------------------------------------------------
    @KafkaListener(
        topics = "notification-events", 
        groupId = "sms-worker-group",
        concurrency = "3" 
    )
    public void processSms(Notification notification) throws InterruptedException {
        if (!"SMS".equalsIgnoreCase(notification.getType())) {
            return;
        }
        
        System.out.println("[SMS WORKER] Preparing to send SMS to User: " + notification.getUserId());
        Thread.sleep(1000); 
        System.out.println("[SMS WORKER] ✅ SUCCESS: SMS sent! Message: " + notification.getMessage());
    }

    // --------------------------------------------------------
    // WORKER 3: PUSH
    // --------------------------------------------------------
    @KafkaListener(
        topics = "notification-events", 
        groupId = "push-worker-group",
        concurrency = "3"
    )
    public void processPush(Notification notification) throws InterruptedException {
        if (!"PUSH".equalsIgnoreCase(notification.getType())) {
            return;
        }
        
        System.out.println("[PUSH WORKER] Preparing to send Push Notification to User: " + notification.getUserId());
        Thread.sleep(1000); 
        System.out.println("[PUSH WORKER] ✅ SUCCESS: Push sent! Message: " + notification.getMessage());
    }


    // --------------------------------------------------------
    // THE DLQ HANDLERS
    // --------------------------------------------------------
    // If it fails all 4 times, Spring automatically routes the message to this method
    @DltHandler
    public void processDltMessage(Notification notificationFromKafka, 
                                  @Header(KafkaHeaders.EXCEPTION_MESSAGE) String errorMessage) {
        System.out.println("\n💀 [DLQ] Message completely failed after 3 retries!");
        
        // Fetch the entity one last time
        repository.findById(notificationFromKafka.getId()).ifPresent(dbNotification -> {
            
            System.out.println("User: " + dbNotification.getUserId());
            System.out.println("Final DB Retry Count was: " + dbNotification.getRetryCount());
            
            // Overwrite the 'type' (or create a new 'status' column in your DB)
            dbNotification.setType("FAILED_PERMANENTLY"); 
            repository.save(dbNotification);
            
            System.out.println("Action: Database row marked as FAILED_PERMANENTLY.\n");
        });
    }
}
