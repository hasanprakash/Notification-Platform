CREATE TABLE user_preference (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL UNIQUE,
    preferences JSONB NOT NULL DEFAULT '{"email": true, "sms": true, "push": true}'
);
