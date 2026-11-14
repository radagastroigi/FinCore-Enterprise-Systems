from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field


app = FastAPI()

class TransferRequest(BaseModel):
    from_acc: str
    to_acc: str
    amount: float = Field(gt=0)

@app.post("/transfer")
def TransferMoney(req: TransferRequest):
    return {
        "status": "TRANSFER_COMPLETED",
        "from_acc": req.from_acc,
        "to_acc": req.to_acc,
        "amount": req.amount
    }

client = TestClient(app)

def test_transfer_validation():
    res_ok = client.post("/transfer",json={
        "from_acc": "ACC-01",
        "to_acc": "ACC-02",
        "amount": 1000.0
    })
    assert res_ok.status_code == 200

    res_fail = client.post("/transfer",json={
        "from_acc": "ACC-01",
        "to_acc": "ACC-02",
        "amount": -1000.0
    })
    assert res_fail.status_code == 422