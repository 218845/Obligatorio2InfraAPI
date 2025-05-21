import os
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
import boto3

# Configuration from environment
DATABASE_URL = os.getenv("DATABASE_URL")  # e.g., postgresql://user:pass@host:5432/dbname
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")  # FIFO queue URL ending in .fifo
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Initialize FastAPI
app = FastAPI(title="Offer Queue API")

# Initialize DB engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Initialize SQS client
sqs = boto3.client("sqs", region_name=AWS_REGION)

class OfferIn(BaseModel):
    id_producto: int = Field(..., description="ID of the product")
    monto: float = Field(..., description="Offer amount")
    usuario: str = Field(..., description="Username placing the offer")
    password: str = Field(..., description="User password for authentication")

class Health(BaseModel):
    status: str = "ok"

@app.get("/health", response_model=Health)
async def health_check():
    """Simple health check endpoint."""
    return Health()

@app.post("/offer")
async def post_offer(offer: OfferIn):
    """
    Authenticate user and send offer to SQS FIFO queue.
    MessageGroupId is id_producto as string.
    Body contains id_producto, usuario, monto.
    """
    # 1. Authenticate user
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT password FROM usuarios WHERE usuario = :user"),
            {"user": offer.usuario}
        )
        row = result.fetchone()

    if not row:
        raise HTTPException(status_code=401, detail="Usuario no encontrado")
    stored_password = row[0]
    if offer.password != stored_password:
        raise HTTPException(status_code=403, detail="Contraseña incorrecta")

    # 2. Prepare message
    message_body = json.dumps({
        "id_producto": offer.id_producto,
        "usuario": offer.usuario,
        "monto": offer.monto
    })

    try:
        # 3. Send to SQS FIFO
        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=message_body,
            MessageGroupId=str(offer.id_producto)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error enviando mensaje a SQS: {e}")

    return {"message_id": response.get("MessageId"), "status": "sent"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
