package com.project.notification_service;

import java.util.TimeZone;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class NotificationServiceApplication {

	public static void main(String[] args) {
		System.out.println("JVM TIMEZONE - BEFORE: " + java.util.TimeZone.getDefault().getID());
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));
		System.out.println("JVM TIMEZONE: AFTER: " + java.util.TimeZone.getDefault().getID());
		SpringApplication.run(NotificationServiceApplication.class, args);
	}

}
