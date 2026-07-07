# Hatem — Ingestion & Simulation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Parent:
> [`../master-plan.md`](../master-plan.md). Milestone: **M1**.

**Goal:** Turn the static Retailrocket dataset into a realistic live event stream on Kafka (with
synthetic surges), stage history into HDFS bronze, and verify zero data loss.

**Architecture:** A Python producer replays `events.csv` ordered by timestamp, serializes to Avro
against Schema Registry, and publishes to `live_web_traffic` at controllable speed with injectable
surge windows. A verifier consumer counts/validates messages. A stager writes CSVs to HDFS bronze.

**Tech Stack:** confluent-kafka, fastavro, pandas, HDFS CLI, pydantic.

## Global Constraints
Inherits [master §Global Constraints](../master-plan.md#global-constraints). Uses only
`libs/scf_common.io.kafka` + `contracts/avro/` — **no hard-coded topic names**. Dataset is free (Kaggle).

## File Structure
- `ingestion/generator/traffic_generator.py` — replay + surge engine (CLI).
- `ingestion/generator/surge.py` — surge schedule model.
- `ingestion/verifier/consumer_verifier.py` — count & validate.
- `ingestion/config/generator.yaml` — speed, surge windows, dataset path.
- `scripts/download_data.sh`, `scripts/hdfs_load.sh` — fetch + stage.
- Tests: `tests/unit/test_surge.py`, `tests/integration/test_producer_roundtrip.py`.

---

### Task 1: Surge schedule model (pure logic, unit-tested)

**Files:** Create `ingestion/generator/surge.py`; Test `tests/unit/test_surge.py`
**Interfaces:**
- Produces: `class SurgeSchedule` with `def multiplier(self, ts: datetime) -> float` — returns the
  speed multiplier active at `ts` (1.0 normal, e.g. 5.0 during a configured surge window).

- [x] **Step 1: Write failing test**
```python
# tests/unit/test_surge.py
from datetime import datetime
from ingestion.generator.surge import SurgeSchedule
def test_multiplier_inside_and_outside_window():
    s = SurgeSchedule(windows=[("2026-01-01T10:00","2026-01-01T11:00", 5.0)])
    assert s.multiplier(datetime(2026,1,1,10,30)) == 5.0
    assert s.multiplier(datetime(2026,1,1,9,0)) == 1.0
```
- [x] **Step 2: Run, expect FAIL** (`ModuleNotFoundError`).
- [x] **Step 3: Implement `SurgeSchedule`** parsing ISO windows, returning the matching multiplier else 1.0.
- [x] **Step 4: Run, expect PASS.**
- [x] **Step 5: Commit** — `feat(ingestion): surge schedule model`.

### Task 2: Avro serialization matches the contract

**Files:** Modify `ingestion/generator/traffic_generator.py`; Test `tests/unit/test_event_avro.py`
**Interfaces:**
- Consumes: `contracts/avro/live_web_traffic.avsc`, `libs/scf_common.io.kafka.AvroKafkaProducer`.
- Produces: `def to_avro_record(row: dict) -> dict` conforming to the schema fields
  `{event_time:long, visitor_id:long, event:string, item_id:long, price:double|null}`.

- [x] **Step 1: Failing test** — build a record from a sample row and assert `fastavro.validate(record,
  schema)` is True and rejects a bad type.
- [x] **Step 2: Run, expect FAIL.**
- [x] **Step 3: Implement `to_avro_record`** mapping Retailrocket columns → schema fields.
- [x] **Step 4: Run, expect PASS.**
- [x] **Step 5: Commit** — `feat(ingestion): contract-conformant avro records`.

### Task 3: Producer round-trips through real Kafka (integration)

**Files:** Modify `ingestion/generator/traffic_generator.py`; Test `tests/integration/test_producer_roundtrip.py`
**Interfaces:** Produces CLI `python -m ingestion.generator.traffic_generator --speed 100 --limit N`.

- [x] **Step 1: Failing test** — with a `testcontainers` Kafka + Apicurio, produce N records then
  consume and assert count == N and first/last `event_time` ordering preserved.
- [x] **Step 2: Run, expect FAIL.**
- [x] **Step 3: Implement** the replay loop: read CSV in timestamp order, apply `SurgeSchedule` to sleep
  intervals (`interval / multiplier`), produce Avro. Flush on exit.
- [x] **Step 4: Run, expect PASS.**
- [x] **Step 5: Commit** — `feat(ingestion): timestamp-ordered producer with surge speed`.

### Task 4: Verifier detects loss/lag

**Files:** Create `ingestion/verifier/consumer_verifier.py`; Test `tests/integration/test_verifier.py`
**Interfaces:** Produces `def verify(expected_count:int, timeout_s:int) -> VerifyReport` with
`received`, `missing`, `max_lag_ms`.

- [x] **Step 1: Failing test** — produce 500, verify reports `missing == 0`.
- [x] **Step 2..4:** implement consumer that counts, tracks offsets, computes lag; PASS.
- [x] **Step 5: Commit** — `feat(ingestion): consumer verifier with loss/lag report`.

### Task 5: Data staging to HDFS bronze

**Files:** `scripts/download_data.sh`, `scripts/hdfs_load.sh`; Test `tests/integration/test_hdfs_stage.py`
**Interfaces:** Produces bronze CSVs at `contracts/hdfs` bronze paths for Emad's ETL.

- [x] **Step 1: Failing test** — after staging, `hdfs dfs -test -e /data/bronze/events` exits 0.
- [x] **Step 2..4:** `download_data.sh` pulls Retailrocket via Kaggle API (free) into `data/raw/`;
  `hdfs_load.sh` `-put`s events + item_properties + category_tree into bronze. PASS.
- [x] **Step 5: Commit** — `feat(ingestion): stage retailrocket into HDFS bronze`.
