package com.project.notification_service.service;

import com.project.notification_service.entity.NotificationPreferences;
import com.project.notification_service.entity.UserPreference;
import com.project.notification_service.repository.UserPreferenceRepository;
import org.springframework.stereotype.Service;

@Service
public class PreferenceService {

    private final UserPreferenceRepository userPreferenceRepository;

    public PreferenceService(UserPreferenceRepository userPreferenceRepository) {
        this.userPreferenceRepository = userPreferenceRepository;
    }

    public boolean checkPreference(String userId, String notificationType) {
        NotificationPreferences preferences = userPreferenceRepository.findByUserId(userId)
                .map(UserPreference::getPreferences)
                .orElse(new NotificationPreferences());

        return switch (notificationType.toUpperCase()) {
            case "EMAIL" -> preferences.isEmail();
            case "SMS" -> preferences.isSms();
            case "PUSH" -> preferences.isPush();
            default -> true;
        };
    }
}
