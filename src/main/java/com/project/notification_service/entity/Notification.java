package com.project.notification_service.entity;

import java.time.LocalDateTime;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "notifications")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Notification {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String userId;
    private String type;
    private String message;
    
    @Column(name = "retry_count", nullable = false)
    private Integer retryCount = 0;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "send_after", nullable = true)
    private LocalDateTime sendAfter;

    @PrePersist
    @PreUpdate
    private void ensureRetryCountAndStatus() {
        if (this.retryCount == null) {
            this.retryCount = 0;
        }
        if (this.status == null) {
            this.status = "PENDING";
        }
    }
}
