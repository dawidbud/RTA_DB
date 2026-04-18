from fastapi import FastAPI
from pydantic import BaseModel
import pickle
import numpy as np

app = FastAPI(title="Fraud Detection API")
model = pickle.load(open('fraud_model.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    hour: int
    is_electronics: int
    tx_per_day: int

class ScoringResult(BaseModel):
    is_fraud: bool
    fraud_probability: float

@app.post('/score', response_model=ScoringResult)
def score(transaction: Transaction):
    features = np.array([[transaction.amount, transaction.hour,
                          transaction.is_electronics, transaction.tx_per_day]])
    prob = float(model.predict_proba(features)[0][1])
    return {"is_fraud": prob >= 0.5, "fraud_probability": prob}

@app.get('/health')
def health():
    return {"status": "ok"}
