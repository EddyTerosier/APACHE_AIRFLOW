CREATE TABLE IF NOT EXISTS tasks (
    id          SERIAL      PRIMARY KEY,
    chat_id     BIGINT      NOT NULL,
    label       TEXT        NOT NULL,
    is_done     BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
    done_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tasks_chat_id ON tasks (chat_id);
CREATE INDEX IF NOT EXISTS idx_tasks_is_done ON tasks (is_done);