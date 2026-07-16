from __future__ import annotations

import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .store import OperatorStore, RiskLevel

DB_PATH = Path(os.getenv("EMPIRE_OPERATOR_DB", "empire_operator.db"))
store = OperatorStore(DB_PATH)
app = FastAPI(
    title="Empire Operator",
    description="Governed multi-worker task control plane with evidence-backed completion.",
    version="0.1.0",
)


class TaskCreate(BaseModel):
    title: str = Field(min_length=3, max_length=200)
    universe: str = Field(min_length=1, max_length=80)
    repo: str = Field(min_length=1, max_length=200)
    action: str = Field(min_length=3, max_length=2000)
    priority: int = Field(default=50, ge=0, le=100)
    risk: RiskLevel = "safe"


class ClaimRequest(BaseModel):
    worker: str = Field(min_length=2, max_length=80)


class FinishRequest(BaseModel):
    success: bool
    kind: str = Field(min_length=2, max_length=80)
    label: str = Field(min_length=2, max_length=80)
    payload: dict[str, Any]


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "service": "empire-operator", "queue": store.summary()}


@app.get("/api/tasks")
def list_tasks() -> list[dict[str, Any]]:
    return [asdict(task) for task in store.list_tasks()]


@app.post("/api/tasks", status_code=201)
def create_task(request: TaskCreate) -> dict[str, Any]:
    task = store.add_task(**request.model_dump())
    return asdict(task)


@app.post("/api/workers/claim")
def claim_task(request: ClaimRequest) -> dict[str, Any] | None:
    task = store.claim_next(request.worker)
    return asdict(task) if task else None


@app.post("/api/tasks/{task_id}/approve")
def approve_task(task_id: str) -> dict[str, Any]:
    try:
        return asdict(store.approve(task_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Task not found") from exc


@app.post("/api/tasks/{task_id}/finish")
def finish_task(task_id: str, request: FinishRequest) -> dict[str, Any]:
    try:
        return asdict(store.finish(task_id, **request.model_dump()))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Task not found") from exc
