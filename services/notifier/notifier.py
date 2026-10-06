"""Send durable notification outbox items to the course service."""

import logging
import os
import time
from pathlib import Path

import psycopg
import requests
from psycopg.rows import dict_row


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("group17-notifier")
DATABASE_URL = os.environ["DATABASE_URL"]
NOTIFICATIONS_URL = os.getenv(
    "NOTIFICATIONS_URL", "https://notifications.cec.dlandau.nl/api/notify"
)
TOKEN = Path(
    os.getenv("NOTIFICATIONS_TOKEN_FILE", "/run/secrets/notification_token")
).read_text(encoding="utf-8").strip()


def send_next(connection: psycopg.Connection, session: requests.Session) -> bool:
    """Claim and send one pending notification; return False when queue is empty."""
    with connection.transaction():
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT id, experiment_id, measurement_id, notification_type,
                       researcher, cipher_data, attempts
                FROM notification_outbox
                WHERE sent_at IS NULL AND next_attempt_at <= now()
                ORDER BY next_attempt_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """
            )
            notification = cursor.fetchone()
            if notification is None:
                return False

            payload = {
                "notification_type": notification["notification_type"],
                "researcher": notification["researcher"],
                "experiment_id": notification["experiment_id"],
                "measurement_id": notification["measurement_id"],
                "cipher_data": notification["cipher_data"],
            }

            try:
                response = session.post(
                    NOTIFICATIONS_URL,
                    params={"token": TOKEN},
                    json=payload,
                    timeout=(2, 3),
                )
                response.raise_for_status()
            except requests.RequestException as error:
                backoff = min(2 ** min(notification["attempts"] + 1, 6), 60)
                # requests exceptions can include the full URL, including its token.
                error_summary = type(error).__name__
                if error.response is not None:
                    error_summary += f" HTTP {error.response.status_code}"
                cursor.execute(
                    """
                    UPDATE notification_outbox
                    SET attempts = attempts + 1,
                        next_attempt_at = now() + (%s * interval '1 second'),
                        last_error = %s
                    WHERE id = %s
                    """,
                    (backoff, error_summary, notification["id"]),
                )
                LOGGER.warning(
                    "notification_failed type=%s experiment=%s retry_seconds=%s",
                    notification["notification_type"],
                    notification["experiment_id"],
                    backoff,
                )
            else:
                cursor.execute(
                    """
                    UPDATE notification_outbox
                    SET sent_at = now(), attempts = attempts + 1, last_error = NULL
                    WHERE id = %s
                    """,
                    (notification["id"],),
                )
                LOGGER.info(
                    "notification_sent type=%s experiment=%s measurement=%s "
                    "response=%s",
                    notification["notification_type"],
                    notification["experiment_id"],
                    notification["measurement_id"],
                    response.text[:100],
                )
    return True


def main() -> None:
    if not TOKEN:
        raise RuntimeError("Notification token is empty")
    with requests.Session() as session:
        while True:
            try:
                with psycopg.connect(DATABASE_URL) as connection:
                    connection.autocommit = True
                    while True:
                        if not send_next(connection, session):
                            time.sleep(0.25)
            except psycopg.Error:
                LOGGER.exception("database_connection_failed")
                time.sleep(2)


if __name__ == "__main__":
    main()
