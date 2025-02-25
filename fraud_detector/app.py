# fraud_detector/app.py
import asyncio
import logging
from transaction_consumer import TransactionConsumer
from fraud_rules import FraudRulesEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    fraud_engine = FraudRulesEngine()
    consumer = TransactionConsumer(fraud_engine)

    try:
        await consumer.start_consuming()
    except Exception as e:
        logger.error(f"Error in main fraud detector service: {e}")
    finally:
        await consumer.stop_consuming()


if __name__ == "__main__":
    logger.info("Starting Fraud Detection Service")
    asyncio.run(main())