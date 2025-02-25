# fraud_detector/transaction_consumer.py
import os
import json
import logging
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionConsumer:
    def __init__(self, fraud_engine):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.transactions_topic = os.getenv("TRANSACTIONS_TOPIC", "transactions")
        self.fraud_alerts_topic = os.getenv("FRAUD_ALERTS_TOPIC", "fraud_alerts")
        self.fraud_engine = fraud_engine
        self.consumer = None
        self.producer = None
        self.running = False

    async def setup(self):
        self.consumer = AIOKafkaConsumer(
            self.transactions_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id="fraud-detector-group",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        await self.consumer.start()
        await self.producer.start()
        logger.info(f"Consumer started for topic: {self.transactions_topic}")

    async def start_consuming(self):
        await self.setup()
        self.running = True

        try:
            async for msg in self.consumer:
                logger.info(f"Received transaction: {msg.value}")
                transaction = msg.value

                # Проверяем транзакцию на признаки мошенничества
                is_fraud, reason = await self.fraud_engine.check_transaction(transaction)

                if is_fraud:
                    alert = {
                        "transaction": transaction,
                        "reason": reason,
                        "timestamp": transaction.get("timestamp")
                    }
                    await self.producer.send_and_wait(
                        self.fraud_alerts_topic,
                        value=alert
                    )
                    logger.warning(f"Fraud detected: {reason}. Alert sent to {self.fraud_alerts_topic}")

                if not self.running:
                    break

        except Exception as e:
            logger.error(f"Error processing transactions: {e}")
            raise

    async def stop_consuming(self):
        self.running = False
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        logger.info("Consumer and producer stopped")