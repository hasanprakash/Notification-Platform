package com.project.notification_service.enums;

public enum NotificationStatus {
    
    PENDING("PENDING"),
    SENT("SENT"),
    FAILED("FAILED"),
    RATE_LIMITED("RATE_LIMITED"),
    SKIPPED("SKIPPED"),
    FAILED_PERMANENTLY("FAILED_PERMANENTLY");

    private final String status;

    NotificationStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }
}
