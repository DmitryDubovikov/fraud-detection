# fraud_detector/fraud_rules.py
import os
import redis
import json
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FraudRulesEngine:
    def __init__(self):
        # Подключение к Redis для хранения истории транзакций
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.redis = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)

        # Настройка правил
        self.amount_threshold = 5000.0  # Транзакции выше этой суммы считаются подозрительными
        self.frequency_threshold = 3  # Более чем 3 транзакции в час считаются подозрительными
        self.time_window = 3600  # Окно времени в секундах (1 час)

        logger.info(f"Fraud Rules Engine initialized. Redis: {self.redis_host}:{self.redis_port}")

    async def check_transaction(self, transaction):
        user_id = transaction.get("user_id")
        amount = transaction.get("amount", 0)
        timestamp = transaction.get("timestamp")
        location = transaction.get("location")

        # Проверяем подозрительно большие транзакции
        if amount > self.amount_threshold:
            return True, f"Amount ({amount}) exceeds threshold of {self.amount_threshold}"

        # Сохраняем транзакцию в историю пользователя
        transaction_key = f"user:{user_id}:transactions"
        transaction_with_time = {
            "amount": amount,
            "timestamp": timestamp,
            "location": location
        }

        # Добавляем транзакцию в список в Redis с TTL 24 часа
        self.redis.lpush(transaction_key, json.dumps(transaction_with_time))
        self.redis.expire(transaction_key, 86400)  # 24 часа

        # Получаем историю транзакций пользователя
        transaction_history = self.redis.lrange(transaction_key, 0, -1)
        transaction_history = [json.loads(t) for t in transaction_history]

        # Проверяем частоту транзакций
        recent_transactions = self._get_recent_transactions(transaction_history, self.time_window)
        if len(recent_transactions) > self.frequency_threshold:
            return True, f"Frequency ({len(recent_transactions)} transactions in {self.time_window / 3600}h) exceeds threshold"

        # Проверка на аномальное местоположение
        if location and len(transaction_history) > 1:
            is_location_suspicious = self._check_location_anomaly(transaction_history, location)
            if is_location_suspicious:
                return True, f"Suspicious location change detected: {location}"

        return False, None

    def _get_recent_transactions(self, transaction_history, time_window_seconds):
        """Получает транзакции в пределах временного окна"""
        now = datetime.now()
        recent = []

        for transaction in transaction_history:
            try:
                tx_time = datetime.fromisoformat(transaction.get("timestamp", ""))
                if (now - tx_time).total_seconds() <= time_window_seconds:
                    recent.append(transaction)
            except (ValueError, TypeError):
                # Пропускаем транзакцию, если не можем спарсить время
                continue

        return recent

    def _check_location_anomaly(self, transaction_history, current_location):
        """Простая проверка на изменение местоположения"""
        # Берем предыдущую транзакцию
        previous_location = None
        for transaction in transaction_history[1:]:  # Пропускаем текущую
            if "location" in transaction:
                previous_location = transaction["location"]
                break

        if previous_location and previous_location != current_location:
            # Очень простая проверка - в реальной системе здесь может быть геолокационный анализ
            logger.info(f"Location change detected: {previous_location} -> {current_location}")
            return True

        return False