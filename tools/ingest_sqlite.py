import argparse
import json
import math
import sqlite3
import sys
from typing import Dict, List, Tuple

import requests
from tqdm import tqdm


def bin_spectrum(mz: List[float], inten: List[float], bin_size: float) -> Tuple[List[int], List[float]]:
    if len(mz) != len(inten):
        raise ValueError("mz_list and intensity_list lengths differ")

    accumulator: Dict[int, float] = {}
    for m, i in zip(mz, inten):
        if i == 0:
            continue
        if m < 0:
            continue
        idx = int(math.floor(m / bin_size))
        accumulator[idx] = accumulator.get(idx, 0.0) + float(i)

    if not accumulator:
        return [], []

    indices = sorted(accumulator.keys())
    values = [accumulator[k] for k in indices]
    # L2 normalize
    norm_sq = sum(v * v for v in values)
    if norm_sq > 0:
        norm = math.sqrt(norm_sq)
        values = [v / norm for v in values]
    return indices, values


def upsert_batch(api_url: str, batch: List[dict]) -> None:
    url = api_url.rstrip("/") + "/upsert"
    payload = {"points": batch}
    resp = requests.post(url, json=payload, timeout=60)
    if resp.status_code >= 300:
        raise RuntimeError(f"Upsert failed: {resp.status_code} {resp.text}")


def iter_rows(conn: sqlite3.Connection, table: str, id_col: str, mz_col: str, intensity_col: str, limit: int) -> Tuple[str, str, str]:
    cur = conn.cursor()
    query = f"SELECT {id_col}, {mz_col}, {intensity_col} FROM {table}"
    if limit > 0:
        query += f" LIMIT {limit}"
    for row in cur.execute(query):
        yield row


def parse_json_array(value: str) -> List[float]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(map(float, value))
    try:
        arr = json.loads(value)
        return list(map(float, arr))
    except Exception as e:
        raise ValueError(f"Failed to parse JSON array: {e}; value startswith: {str(value)[:80]}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest mass spectra from SQLite into Qdrant via API")
    parser.add_argument("db_path", help="Path to SQLite database file")
    parser.add_argument("table", help="Table name containing spectra")
    parser.add_argument("id_col", help="ID column name")
    parser.add_argument("mz_col", help="Column with JSON array of m/z values")
    parser.add_argument("intensity_col", help="Column with JSON array of intensities")

    parser.add_argument("--api-url", default="http://localhost:8000", help="Base URL of the FastAPI service")
    parser.add_argument("--bin-size", type=float, default=0.1, help="m/z bin size, e.g., 0.1")
    parser.add_argument("--batch-size", type=int, default=256, help="Number of spectra per upsert batch")
    parser.add_argument("--limit", type=int, default=-1, help="Limit rows ingested (for testing)")

    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    total = 0
    batch: List[dict] = []

    try:
        rows = list(iter_rows(conn, args.table, args.id_col, args.mz_col, args.intensity_col, args.limit))
        for rid, mz_json, inten_json in tqdm(rows, desc="Ingesting"):
            try:
                mz = parse_json_array(mz_json)
                intensities = parse_json_array(inten_json)
            except Exception as e:
                tqdm.write(f"[skip] id={rid} parse error: {e}")
                continue

            indices, values = bin_spectrum(mz, intensities, args.bin_size)
            if not indices:
                tqdm.write(f"[skip] id={rid} empty spectrum after binning")
                continue

            point = {
                "id": rid,
                "sparse": {"indices": indices, "values": values},
                "metadata": {}
            }
            batch.append(point)
            if len(batch) >= args.batch_size:
                upsert_batch(args.api_url, batch)
                total += len(batch)
                batch.clear()

        if batch:
            upsert_batch(args.api_url, batch)
            total += len(batch)
            batch.clear()

        print(f"Ingested {total} spectra")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())


