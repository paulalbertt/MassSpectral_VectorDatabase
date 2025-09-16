# MassSpectral_VectorDatabase
Code to develop and update a vector database for mass spectra
# Mass Spectral Vector Database (Qdrant + FastAPI)

This repository provides a minimal setup to store and search mass spectra as sparse vectors using Qdrant, with a FastAPI service exposing simple endpoints to upsert and search spectra.

## Stack

- Qdrant (vector DB) with sparse vector indexing
- FastAPI (Python) for API endpoints
- Docker Compose for local development

## Run locally

1) Build and start services

```bash
docker compose up -d --build
```

This starts:
- Qdrant at `http://localhost:6333`
- API at `http://localhost:8000`

## API

Open interactive docs: `http://localhost:8000/docs`

### Health

```http
GET /health
```

### Upsert spectra

Sparse vectors are represented by indices (non-zero bins) and values (intensities). You can optionally attach metadata.

```http
POST /upsert
Content-Type: application/json

{
  "points": [
    {
      "id": "spec_1",
      "sparse": {"indices": [15, 102, 450], "values": [0.12, 0.85, 0.33]},
      "metadata": {"name": "Compound A", "precursor_mz": 321.12}
    },
    {
      "id": "spec_2",
      "sparse": {"indices": [10, 77], "values": [0.5, 0.9]},
      "metadata": {"name": "Compound B"}
    }
  ]
}
```

### Search (similarity)

```http
POST /search
Content-Type: application/json

{
  "query": {"indices": [15, 102], "values": [0.1, 0.9]},
  "limit": 5,
  "with_payload": true
}
```

Response returns a hitlist sorted by similarity score (higher is more similar):

```json
{
  "hits": [
    {"id": "spec_2", "score": 0.72, "payload": {"name": "Compound B"}},
    {"id": "spec_1", "score": 0.65, "payload": {"name": "Compound A"}}
  ]
}
```

## Notes

- Collection name defaults to `ms_spectra`. Override via env var `QDRANT_COLLECTION` on the API service.
- By default, sparse vectors are stored under named vector `"sparse"` with on-disk index.
- Modify scoring or add filters by extending the API handler in `app/main.py`.

## Ingest spectra from a SQLite database

The repo includes a simple CLI to populate Qdrant via the API from a SQLite database with columns: `id`, `mz_list` (JSON array), and `intensity_list` (JSON array).

Run (with Docker started and the stack up):

```bash
python3 tools/ingest_sqlite.py path/to/db.sqlite spectra_table id mz_list intensity_list \
  --api-url http://localhost:8000 \
  --bin-size 0.1 \
  --batch-size 256
```

Assumptions:

- `mz_list` and `intensity_list` are stored as JSON arrays (e.g., `[100.1, 101.2, ...]`).
- Binning uses `bin_index = floor(mz / bin_size)` and sums intensities per bin to produce a sparse vector.

To adapt to other sources (CSV, mzML), you can write a similar script that outputs the same request body to the `/upsert` endpoint.

## Ingest spectra from MSP files

Use the included CLI to parse `.msp` files, bin peaks, and upload to the API.

```bash
python3 tools/ingest_msp.py /User/VectorDatabases/database_file/MSMS-Public_experimentspectra-pos-VS19.msp \
  --api-url http://localhost:8000 \
  --bin-size 0.1 \
  --batch-size 256 \
  --id-key NAME
```

Notes:

- The parser looks for `Num Peaks:` and then reads lines of `mz intensity` pairs until a blank line.
- Metadata lines before `Num Peaks:` are captured as `key: value`. The `--id-key` selects which metadata field to use as the spectrum ID.
- Binning uses `bin_index = floor(mz / bin_size)` and sums intensities per bin.
