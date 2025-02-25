# alert_service/alert_consumer.py
import os
import json
import logging
import asyncio
import requests
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlertConsumer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.topic = os.getenv("FRAUD_ALERTS_TOPIC", "fraud_alerts")
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.consumer = None
        self.running = False

    async def setup(self):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id="alert-service-group",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        await self.consumer.start()
        logger.info(f"Alert consumer started for topic: {self.topic}")

    async def start_consuming(self):
        await self.setup()
        self.running = True

        try:
            async for msg in self.consumer:
                logger.info(f"Received fraud alert: {msg.value}")
                alert = msg.value

                # Логирование алерта
                self._log_alert(alert)

                # Отправка уведомления (если настроено)
                await self._send_notification(alert)

                if not self.running:
                    break

        except Exception as e:
            logger.error(f"Error processing alerts: {e}")
            raise

    async def stop_consuming(self):
        self.running = False
        if self.consumer:
            await self.consumer.stop()
        logger.info("Alert consumer stopped")

    def _log_alert(self, alert):
        """Логирует информацию о мошенничестве"""
        transaction = alert.get("transaction", {})
        reason = alert.get("reason", "Unknown reason")

        log_message = (
            f"FRAUD ALERT:\n"
            f"User ID: {transaction.get('user_id')}\n"
            f"Amount: {transaction.get('amount')} {transaction.get('currency', 'USD')}\n"
            f"Timestamp: {transaction.get('timestamp')}\n"
            f"Reason: {reason}\n"
            f"Location: {transaction.get('location', 'Unknown')}\n"
            f"Details: {json.dumps(transaction, indent=2)}"
        )

        logger.warning(log_message)

    async def _send_notification(self, alert):
        """Отправляет уведомление через Telegram или другой сервис"""
        # Примечание: В реальной системе здесь может быть интеграция с Telegram, Email, SMS и т.д.
        if self.telegram_token and self.telegram_chat_id:
            transaction = alert.get("transaction", {})
            reason = alert.get("reason", "Unknown reason")

            message = (
                f"🚨 FRAUD ALERT 🚨\n\n"
                f"User: {transaction.get('user_id')}\n"
                f"Amount: {transaction.get('amount')} {transaction.get('currency', 'USD')}\n"
                f"Reason: {reason}\n"
                f"Location: {transaction.get('location', 'Unknown')}\n"
                f"Time: {transaction.get('timestamp')}"
            )

            try:
                # Асинхронное выполнение HTTP-запроса в отдельном потоке
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    self._send_telegram_message,
                    message
                )
                logger.info("Telegram notification sent")
            except Exception as e:
                logger.error(f"Failed to send Telegram notification: {e}")

    def _send_telegram_message(self, message):
        """Отправляет сообщение в Telegram"""
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        data = {
            "chat_id": self.telegram_chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }

        response = requests.post(url, data=data)
        if response.status_code != 200:
            logger.error(f"Telegram API error: {response.text}")