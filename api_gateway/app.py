import json
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from transaction_producer import TransactionProducer

app = FastAPI(title="Fraud Detection API Gateway")
producer = TransactionProducer()


class Transaction(BaseModel):
    user_id: str
    amount: float
    currency: str = "USD"
    timestamp: Optional[str] = None
    source_ip: Optional[str] = None
    device_id: Optional[str] = None
    location: Optional[str] = None

    class Config:
        schema_extra = {
            "example": {
                "user_id": "12345",
                "amount": 1000.00,
                "currency": "USD",
                "device_id": "device_123",
                "location": "New York, USA"
            }
        }


@app.post("/api/v1/transactions")
async def create_transaction(transaction: Transaction, background_tasks: BackgroundTasks):
    if not transaction.timestamp:
        transaction.timestamp = datetime.now().isoformat()

    # Отправляем транзакцию в Kafka
    background_tasks.add_task(
        producer.send_transaction,
        transaction.dict()
    )
    return {"status": "success", "message": "Transaction submitted for processing"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)