ALTER TABLE notifications
ADD COLUMN retry_count integer DEFAULT 0;