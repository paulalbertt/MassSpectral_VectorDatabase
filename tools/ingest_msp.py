import argparse
import math
import uuid
from typing import Dict, Iterator, List, Tuple

import requests
from tqdm import tqdm


def parse_msp(file_path: str) -> Iterator[dict]:
    """Yield spectra from an MSP file as dictionaries with metadata and peaks.

    MSP fields vary; we parse common headers and the peak list after 'Num Peaks:'.
    Each spectrum is separated by a blank line.
    """
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        meta: Dict[str, str] = {}
        peaks: List[Tuple[float, float]] = []
        in_peaks = False

        def yield_current():
            if peaks:
                yield {
                    "metadata": meta.copy(),
                    "peaks": peaks.copy(),
                }

        for raw in f:
            line = raw.strip()
            if not line:
                # spectrum separator
                if peaks or meta:
                    for item in yield_current():
                        yield item
                    meta.clear()
                    peaks.clear()
                    in_peaks = False
                continue

            if not in_peaks:
                if ":" in line:
                    key, value = line.split(":", 1)
                    key = key.strip()
                    value = value.strip()
                    if key.lower() == "num peaks":
                        in_peaks = True
                        continue
                    meta[key] = value
                else:
                    # unexpected line; ignore or treat as metadata continuation
                    continue
            else:
                # peaks lines: "mz intensity" pairs separated by whitespace or tabs
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        mz = float(parts[0])
                        inten = float(parts[1])
                        peaks.append((mz, inten))
                    except ValueError:
                        # non-numeric line within peaks; skip
                        continue

        # flush last spectrum
        if peaks or meta:
            for item in yield_current():
                yield item


def bin_peaks(peaks: List[Tuple[float, float]], bin_size: float) -> Tuple[List[int], List[float]]:
    accumulator: Dict[int, float] = {}
    for mz, inten in peaks:
        if inten <= 0 or mz < 0:
            continue
        idx = int(math.floor(mz / bin_size))
        accumulator[idx] = accumulator.get(idx, 0.0) + float(inten)
    if not accumulator:
        return [], []
    indices = sorted(accumulator.keys())
    values = [accumulator[i] for i in indices]
    # L2 normalize
    norm_sq = sum(v * v for v in values)
    if norm_sq > 0:
        norm = math.sqrt(norm_sq)
        values = [v / norm for v in values]
    return indices, values


def upsert_batch(api_url: str, batch: List[dict]) -> None:
    url = api_url.rstrip("/") + "/upsert"
    payload = {"points": batch}
    resp = requests.post(url, json=payload, timeout=120)
    if resp.status_code >= 300:
        raise RuntimeError(f"Upsert failed: {resp.status_code} {resp.text}")


def main() -> int:
    p = argparse.ArgumentParser(description="Ingest MSP file into Qdrant via API")
    p.add_argument("msp_path", help="Path to .msp file")
    p.add_argument("--api-url", default="http://localhost:8000", help="Base URL of the FastAPI service")
    p.add_argument("--bin-size", type=float, default=0.1, help="m/z bin size (e.g., 0.1)")
    p.add_argument("--batch-size", type=int, default=256, help="Spectra per upsert batch")
    p.add_argument("--id-key", default=None, help="(Optional) metadata key to also copy into payload as original_id")
    args = p.parse_args()

    batch: List[dict] = []
    total = 0

    for spec in tqdm(parse_msp(args.msp_path), desc="Parsing MSP"):
        peaks = spec.get("peaks", [])
        indices, values = bin_peaks(peaks, args.bin_size)
        if not indices:
            continue
        meta = spec.get("metadata", {})
        point_id = str(uuid.uuid4())
        # Optionally keep a reference to an original identifier inside payload
        if args.id_key and (orig := meta.get(args.id_key)) is not None:
            meta.setdefault("original_id", orig)

        point = {
            "id": point_id,
            "sparse": {"indices": indices, "values": values},
            "metadata": meta,
        }
        batch.append(point)
        if len(batch) >= args.batch_size:
            upsert_batch(args.api_url, batch)
            total += len(batch)
            batch.clear()

    if batch:
        upsert_batch(args.api_url, batch)
        total += len(batch)

    print(f"Ingested {total} spectra from {args.msp_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


