package com.project.notification_service.service;

import java.time.LocalDateTime;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.project.notification_service.entity.Notification;
import com.project.notification_service.enums.NotificationStatus;
import com.project.notification_service.repository.NotificationRepository;

@Service
public class NotificationConsumer {

    private static final Logger logger = LoggerFactory.getLogger(NotificationConsumer.class);
    private final NotificationRepository notificationRepo;
    private final RateLimiterService rateLimiterService;
    public NotificationConsumer(NotificationRepository notificationRepo, RateLimiterService rateLimiterService) {
        this.notificationRepo = notificationRepo;
        this.rateLimiterService = rateLimiterService;
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
    // @Transactional is required so the DB lock stays active for the whole method!
    @Transactional(noRollbackFor = RuntimeException.class)
    @KafkaListener(topics = "notification-events", groupId = "email-worker-group", concurrency = "3")
    public void processEmail(Notification notificationFromKafka, @Header("trace_id") String traceId) throws InterruptedException {
        if (!"EMAIL".equalsIgnoreCase(notificationFromKafka.getType())) {
            return; 
        }

        try {
            MDC.put("traceId", traceId);

            // 1. ACQUIRE THE LOCK
            // If Thread A and B arrive instantly, Thread A gets the row.
            // Thread B is literally frozen here until Thread A finishes.
            Notification dbNotification = notificationRepo.findByIdForUpdate(notificationFromKafka.getId())
                .orElseThrow(() -> new RuntimeException("Not found!"));
            
            // 2. CHECK STATUS (The actual idempotency check)
            if (!"PENDING".equals(dbNotification.getStatus())) {
                logger.info("Already processed or Already Rate Limited! Ignoring.");
                return;
            }

            // 3. CHECK RATE LIMIT - random failures are counted as rate limit hits
            if (!rateLimiterService.isAllowed(dbNotification.getUserId())) {
                logger.info("Rate limit hit! Pausing message for 1 hour.");
                dbNotification.setStatus("RATE_LIMITED");
                dbNotification.setSendAfter(LocalDateTime.now().plusHours(1));
                notificationRepo.save(dbNotification);
                return; // Exit gracefully. Do not throw an exception!
            }

            logger.info("[EMAIL WORKER] Preparing to send Email to User: {}", dbNotification.getUserId());

            // If DB says retryCount is 0, this is Attempt #1. If retryCount is 1, it's Attempt #2.
            int currentAttempt = dbNotification.getRetryCount() + 1;
            logger.info("[EMAIL WORKER] Attempt #{} for User: {}", currentAttempt, dbNotification.getUserId());

            // 4. SIMULATE A RANDOM FAILURE
            boolean randomFailure = new Random().nextBoolean();
            if (randomFailure) {
                logger.warn("CRASH! Email Provider is down.");
                
                // Increment the counter in the DB and save it BEFORE throwing the exception
                dbNotification.setRetryCount(dbNotification.getRetryCount() + 1);
                notificationRepo.save(dbNotification);
                
                // Now throw the exception so Kafka moves it to the next retry topic
                throw new RuntimeException("Email Provider Failed!"); 
            }

            // 5. MARK SUCCESS
            Thread.sleep(1000); 
            dbNotification.setStatus(NotificationStatus.SENT.getStatus());
            notificationRepo.save(dbNotification);
            logger.info("SUCCESS: Email sent! Message: {}", dbNotification.getMessage());
        }
        finally {
            MDC.clear(); // Clear the MDC after the request is processed
        }
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
        
        logger.info("[SMS WORKER] Preparing to send SMS to User: {}", notification.getUserId());
        Thread.sleep(1000); 
        logger.info("[SMS WORKER] SUCCESS: SMS sent! Message: {}", notification.getMessage());
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
        
        logger.info("[PUSH WORKER] Preparing to send Push Notification to User: {}", notification.getUserId());
        Thread.sleep(1000); 
        logger.info("[PUSH WORKER] SUCCESS: Push sent! Message: {}", notification.getMessage());
    }


    // --------------------------------------------------------
    // THE DLQ HANDLERS
    // --------------------------------------------------------
    // If it fails all 4 times, Spring automatically routes the message to this method
    @DltHandler
    public void processDltMessage(Notification notificationFromKafka, 
                                  @Header(KafkaHeaders.EXCEPTION_MESSAGE) String errorMessage) {
        logger.error("[DLQ] Message completely failed after 3 retries!");
        
        // Fetch the entity one last time
        notificationRepo.findById(notificationFromKafka.getId()).ifPresent(dbNotification -> {
            
            logger.error("User: {}", dbNotification.getUserId());
            logger.error("Final DB Retry Count was: {}", dbNotification.getRetryCount());
            
            // Overwrite the 'type' (or create a new 'status' column in your DB)
            dbNotification.setStatus("FAILED_PERMANENTLY"); 
            notificationRepo.save(dbNotification);
            
            logger.error("Action: Database row marked as FAILED_PERMANENTLY.");
        });
    }
}
