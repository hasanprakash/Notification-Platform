package com.project.notification_service.repository;

import com.project.notification_service.entity.Notification;
import com.project.notification_service.enums.NotificationStatus;

import jakarta.persistence.LockModeType;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface NotificationRepository extends JpaRepository<Notification, Long> {

    // It locks the row in Postgres. If Thread B tries to run this while Thread A 
    // is processing, Thread B is forced to wait in line.
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT n FROM Notification n WHERE n.id = :id")
    Optional<Notification> findByIdForUpdate(@Param("id") Long id);

    // Find messages where status is RATE_LIMITED and the send_after time has passed
    @Query("SELECT n FROM Notification n WHERE n.status = :status AND n.sendAfter < :now")
    List<Notification> findByStatusAndSendAfterBefore(@Param("status") String status, @Param("now") LocalDateTime now);
}
