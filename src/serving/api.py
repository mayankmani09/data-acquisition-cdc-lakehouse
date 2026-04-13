from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="Acquisition CDC Lakehouse API")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/kpi/daily-revenue")
def daily_revenue() -> dict[str, str]:
    return {"message": "Wire this endpoint to Gold mart output in a later step."}
