import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from json import JSONDecodeError, loads
from typing import Any

from confluent_kafka import Consumer

logger = logging.getLogger()


@dataclass
class Config:
    sender = "localhost@localhost"
    receiver = "receiver@gmail.com"
    smtp_host = "maildev"
    smtp_port = 1025
    kafka_host = "kafka"
    kafka_port = 9094
    kafka_group = "notifier"
    kafka_topics = ["log-alerts"]


@dataclass
class Event:
    name: str
    start: str
    end: str
    errorCount: int


def load_json(data: str) -> dict[str, Any] | None:
    try:
        decoded_message = loads(data)
    except JSONDecodeError as error:
        logging.error(error)
    else:
        if isinstance(decoded_message, dict):
            return decoded_message
        logging.error("Error loading event message data %s", data)


def decode_event(message: dict[str, Any]) -> Event | None:
    try:
        message["errorCount"] = int(message["errorCount"])
        return Event(**message)
    except (TypeError, KeyError, ValueError) as error:
        logging.error("Error decoding event data %s, %s", message, error)


def compose_alert_message(event: Event, config: Config) -> EmailMessage:
    msg = EmailMessage()
    msg.set_content(f"Alert {event.name}!\nIn period from {event.start} " f"to {event.end}\n{event.errorCount} errors received.")
    msg["Subject"] = f"Alert {event.name}!"
    msg["From"] = config.sender
    msg["To"] = config.receiver
    return msg


def send_email(message: EmailMessage, config: Config) -> None:
    try:
        s = smtplib.SMTP(config.smtp_host, config.smtp_port)
        s.send_message(message)
        s.quit()
    except smtplib.SMTPNotSupportedError as error:
        logging.error("Unable to send email %s", error)


def create_kafka_consumer(config: Config) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": f"{config.kafka_host}:{config.kafka_port}",
            "group.id": config.kafka_group,
            "auto.offset.reset": "earliest",
        }
    )


def processing_loop(consumer: Consumer, config: Config) -> None:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error("Consumer error: %s", msg.error())
            continue
        json_data = load_json(msg.value().decode("utf-8"))
        if not json_data:
            continue
        event = decode_event(json_data)
        if not event:
            continue
        if event.errorCount >= 10:
            send_email(compose_alert_message(event, config), config)


def main() -> None:
    config = Config()
    consumer = create_kafka_consumer(config)
    consumer.subscribe(config.kafka_topics)
    try:
        processing_loop(consumer, config)
    except Exception as error:
        logging.error("Error processing notifications %s", error)
    finally:
        consumer.close()


if __name__ == "__main__":
    while True:
        main()
