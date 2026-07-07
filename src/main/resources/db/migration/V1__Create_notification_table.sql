CREATE TABLE notifications (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(255),
    type VARCHAR(255),
    message VARCHAR(255)
);