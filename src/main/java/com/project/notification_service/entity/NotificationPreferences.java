package com.project.notification_service.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NotificationPreferences {

    private boolean email = true;
    private boolean sms = true;
    private boolean push = true;
}
