import argparse
import csv
import math
from typing import Dict, List, Tuple

import requests


def bin_spectrum(mz: List[float], inten: List[float], bin_size: float) -> Tuple[List[int], List[float]]:
    if len(mz) != len(inten):
        raise ValueError("mz and intensity lengths differ")
    acc: Dict[int, float] = {}
    for m, i in zip(mz, inten):
        if i <= 0 or m < 0:
            continue
        idx = int(math.floor(m / bin_size))
        acc[idx] = acc.get(idx, 0.0) + float(i)
    if not acc:
        return [], []
    indices = sorted(acc.keys())
    values = [acc[k] for k in indices]
    # L2 normalize
    norm_sq = sum(v * v for v in values)
    if norm_sq > 0:
        norm = math.sqrt(norm_sq)
        values = [v / norm for v in values]
    return indices, values


def read_csv_pairs(path: str) -> Tuple[List[float], List[float]]:
    mz: List[float] = []
    inten: List[float] = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 2:
                continue
            try:
                m = float(row[0])
                i = float(row[1])
            except ValueError:
                continue
            mz.append(m)
            inten.append(i)
    return mz, inten


def main() -> int:
    p = argparse.ArgumentParser(description="Search CSV spectrum against API")
    p.add_argument("csv_path", help="Path to CSV with two columns: m/z,intensity")
    p.add_argument("--api-url", default="http://localhost:8000", help="API base URL")
    p.add_argument("--bin-size", type=float, default=0.1, help="Bin size used in ingestion")
    p.add_argument("--limit", type=int, default=10, help="Top-k results to return")
    args = p.parse_args()

    mz, inten = read_csv_pairs(args.csv_path)
    indices, values = bin_spectrum(mz, inten, args.bin_size)
    if not indices:
        print("Empty spectrum after binning; nothing to search.")
        return 1

    url = args.api_url.rstrip("/") + "/search"
    body = {"query": {"indices": indices, "values": values}, "limit": args.limit, "with_payload": True}
    resp = requests.post(url, json=body, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    for i, hit in enumerate(data.get("hits", []), 1):
        pid = hit.get("id")
        score = hit.get("score")
        payload = hit.get("payload")
        name = payload.get("NAME") if isinstance(payload, dict) else None
        print(f"{i}. id={pid} score={score:.6f} name={name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


