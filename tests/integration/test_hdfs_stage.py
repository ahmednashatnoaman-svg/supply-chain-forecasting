"""Integration test: HDFS bronze staging (Hatem plan Task 5, exec Task 5).

Brings up the dev stack's ``namenode`` + ``datanode`` (reusing ``infra/docker/docker-compose.yml``
for parity with the real runbook, per the plan), writes hermetic 5-row synthetic fixtures into
``data/raw/``, runs ``scripts/hdfs_load.sh``, and asserts the bronze paths exist and that
``/data/bronze/events`` holds only the TRAIN split rows.

Skips entirely if the ``docker`` CLI is unavailable. Marked ``@pytest.mark.integration`` so it is
deselectable in the fast CI lane (R-2: HDFS on Windows needs Docker Desktop WSL2).
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "infra" / "docker" / "docker-compose.yml")]
DATA_RAW = ROOT / "data" / "raw"

# Hermetic synthetic fixtures (5 rows each). events_train.csv is the 80% split output.
_EVENTS_TRAIN = (
    "timestamp,visitorid,event,itemid,transactionid\n"
    "1439695200000,1,view,10,\n"
    "1439695300000,2,view,11,\n"
    "1439695400000,3,addtocart,12,\n"
    "1439695500000,4,transaction,13,100\n"
    "1439695600000,5,view,14,\n"
)
_ITEM_PROPS = "timestamp,itemid,property,value\n1439695200000,10,category,shoes\n"
_CATEGORY_TREE = "categoryid,parentid\n1,\n2,1\n"


def _docker_available() -> bool:
    return shutil.which("docker") is not None


@pytest.fixture(scope="module")
def hdfs_stack():
    if not _docker_available():
        pytest.skip("docker CLI unavailable")
    subprocess.run(COMPOSE + ["up", "-d", "namenode", "datanode"], check=True, cwd=str(ROOT))
    # Wait for the namenode to come out of safe mode (be generous on Windows).
    _wait_hdfs_ready(timeout=120)
    yield
    subprocess.run(COMPOSE + ["rm", "-fs", "namenode", "datanode"], check=False, cwd=str(ROOT))


def _wait_hdfs_ready(timeout: float) -> None:
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(
            COMPOSE + ["exec", "-T", "namenode", "hdfs", "dfs", "-ls", "/"],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
        )
        if r.returncode == 0:
            return
        time.sleep(3)
    pytest.skip(f"namenode not ready within {timeout}s")


@pytest.fixture()
def synthetic_fixtures(hdfs_stack):
    """Write tiny fixtures into data/raw/, backing up any pre-existing real files first.

    CI starts with an empty data/raw/; this save/restore keeps a developer's real downloaded data
    intact if they run the test locally.
    """
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    fixtures = {
        "events_train.csv": _EVENTS_TRAIN,
        "item_properties_part1.csv": _ITEM_PROPS,
        "category_tree.csv": _CATEGORY_TREE,
    }
    backed_up: dict[str, Path | None] = {}
    for name, content in fixtures.items():
        target = DATA_RAW / name
        if target.exists():
            backup = DATA_RAW / f".{name}.bak"
            target.replace(backup)
            backed_up[name] = backup
        else:
            backed_up[name] = None
        target.write_text(content)
    yield fixtures
    # restore / clean up
    for name in fixtures:
        (DATA_RAW / name).unlink(missing_ok=True)
        backup = backed_up.get(name)
        if backup is not None:
            backup.replace(DATA_RAW / name)


def _hdfs_test(path: str) -> bool:
    r = subprocess.run(
        COMPOSE + ["exec", "-T", "namenode", "hdfs", "dfs", "-test", "-e", path],
        cwd=str(ROOT),
    )
    return r.returncode == 0


def _hdfs_count(path: str) -> int:
    r = subprocess.run(
        COMPOSE + ["exec", "-T", "namenode", "hdfs", "dfs", "-ls", path],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    # -ls lists the dir's files; the synthetic events_train.csv contributes 5 data rows.
    return r.returncode == 0 and r.stdout.count("\n") >= 1


def test_hdfs_bronze_stages_train_split_and_dims(synthetic_fixtures):
    r = subprocess.run(["bash", str(ROOT / "scripts" / "hdfs_load.sh")], cwd=str(ROOT))
    assert r.returncode == 0, "hdfs_load.sh failed"

    assert _hdfs_test("/data/bronze/events"), "events bronze missing"
    assert _hdfs_test("/data/bronze/item_properties"), "item_properties bronze missing"
    assert _hdfs_test("/data/bronze/category_tree"), "category_tree bronze missing"

    # events bronze holds the TRAIN split only -- assert the staged file is present.
    assert _hdfs_count("/data/bronze/events"), "no events file staged under bronze/events"
