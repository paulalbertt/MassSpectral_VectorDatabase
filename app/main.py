from __future__ import annotations

import os
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from qdrant_client import QdrantClient
from qdrant_client.http import models as rest


QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "ms_spectra")

app = FastAPI(title="Mass Spectra Vector DB API", version="0.1.0")


class SparseVector(BaseModel):
    indices: List[int] = Field(..., description="Indices of non-zero bins", min_length=1)
    values: List[float] = Field(..., description="Corresponding intensities", min_length=1)

    def to_rest(self) -> rest.SparseVector:
        return rest.SparseVector(indices=self.indices, values=self.values)


class SpectrumPoint(BaseModel):
    id: Optional[str | int] = Field(None, description="Optional custom ID")
    sparse: SparseVector
    metadata: Optional[dict] = Field(default_factory=dict)


class UpsertRequest(BaseModel):
    points: List[SpectrumPoint] = Field(..., min_length=1)


class SearchRequest(BaseModel):
    query: SparseVector
    limit: int = 10
    with_payload: bool = True


def get_client() -> QdrantClient:
    return QdrantClient(url=QDRANT_URL)


def ensure_collection(client: QdrantClient):
    try:
        client.get_collection(COLLECTION_NAME)
    except Exception:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=None,
            sparse_vectors_config={
                "sparse": rest.SparseVectorParams(
                    index=rest.SparseIndexParams(
                        on_disk=True
                    )
                )
            },
        )


@app.on_event("startup")
def startup_event():
    client = get_client()
    ensure_collection(client)


@app.post("/upsert")
def upsert_points(req: UpsertRequest):
    client = get_client()
    ensure_collection(client)

    operations = []
    for p in req.points:
        kwargs: dict = {
            "payload": p.metadata,
            "vector": {"sparse": p.sparse.to_rest()},
        }
        if p.id is not None:
            kwargs["id"] = p.id
        operations.append(rest.PointStruct(**kwargs))

    result = client.upsert(
        collection_name=COLLECTION_NAME,
        points=operations,
        wait=True,
    )

    return {"status": "ok", "result": result}


@app.post("/search")
def search(req: SearchRequest):
    client = get_client()

    query = rest.NamedSparseVector(name="sparse", vector=req.query.to_rest())
    try:
        res = client.search(
            collection_name=COLLECTION_NAME,
            query_vector=query,
            limit=req.limit,
            with_payload=req.with_payload,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    hits = []
    for r in res:
        hits.append(
            {
                "id": r.id,
                "score": r.score,
                "payload": r.payload if req.with_payload else None,
            }
        )
    return {"hits": hits}


@app.get("/health")
def health():
    client = get_client()
    try:
        info = client.get_collection(COLLECTION_NAME)
        return {"status": "ok", "collection": COLLECTION_NAME}
    except Exception:
        return {"status": "degraded", "collection": COLLECTION_NAME}


