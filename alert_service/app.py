# alert_service/app.py
import asyncio
import logging
from alert_consumer import AlertConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    consumer = AlertConsumer()

    try:
        await consumer.start_consuming()
    except Exception as e:
        logger.error(f"Error in alert service: {e}")
    finally:
        await consumer.stop_consuming()


if __name__ == "__main__":
    logger.info("Starting Fraud Alert Service")
    asyncio.run(main())