package com.project.notification_service.service;

import com.project.notification_service.entity.Notification;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {
// --------------------------------------------------------
    // WORKER 1: EMAIL (Family 1)
    // --------------------------------------------------------
    @KafkaListener(
        topics = "notification-events", 
        groupId = "email-worker-group",
        concurrency = "3" // This tells Spring to spin up 3 parallel workers for this group!
    )
    public void processEmail(Notification notification) throws InterruptedException {
        if (!"EMAIL".equalsIgnoreCase(notification.getType())) {
            return; // Not my job, ignore it!
        }
        
        System.out.println("[EMAIL WORKER] Preparing to send Email to User: " + notification.getUserId());
        // Simulate hard work
        Thread.sleep(1000); 
        System.out.println("[EMAIL WORKER] ✅ SUCCESS: Email sent! Message: " + notification.getMessage());
    }

    // --------------------------------------------------------
    // WORKER 2: SMS (Family 2)
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
    // WORKER 3: PUSH (Family 3)
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
}
