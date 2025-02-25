import os
import json
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.topic = os.getenv("TRANSACTIONS_TOPIC", "transactions")

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        logger.info(f"Producer initialized: {self.bootstrap_servers}")

    async def send_transaction(self, transaction_data):
        user_id = transaction_data.get("user_id")
        try:
            future = self.producer.send(
                self.topic,
                key=user_id,
                value=transaction_data
            )
            record_metadata = future.get(timeout=10)
            logger.info(f"Transaction sent to topic {record_metadata.topic}, partition {record_metadata.partition}")
            return True
        except Exception as e:
            logger.error(f"Error sending transaction to Kafka: {e}")
            return False