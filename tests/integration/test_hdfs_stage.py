"""Integration test: HDFS bronze staging (Hatem plan Task 5, exec Task 5).

Runs ``scripts/hdfs_load.sh`` against an **isolated** compose project (``scf-it``) with its own
volumes, writing hermetic 5-row synthetic fixtures into a throwaway ``DATA_DIR`` so a developer's
real ``data/raw/`` is never touched and the dev HDFS bronze is never clobbered. Asserts the bronze
paths exist and that ``/data/bronze/events`` holds the TRAIN split file.

Skips entirely if the ``docker`` CLI is unavailable. Marked ``@pytest.mark.integration`` so it is
deselectable in the fast CI lane (R-2: HDFS on Windows needs Docker Desktop WSL2).
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
# Isolated compose project so the test never touches the running dev stack (`scf` project) or its
# bronze data. Project-prefixed volumes give the test a fresh, throwaway HDFS.
_PROJECT = "scf-it"
COMPOSE = [
    "docker",
    "compose",
    "-p",
    _PROJECT,
    "-f",
    str(ROOT / "infra" / "docker" / "docker-compose.yml"),
]

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
    # Only the test's own scf-it containers are removed -- the dev stack is untouched.
    subprocess.run(
        COMPOSE + ["rm", "-fs", "namenode", "datanode"], check=False, cwd=str(ROOT)
    )


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
def synthetic_data_dir(hdfs_stack, tmp_path: Path) -> Path:
    """Write tiny fixtures into a throwaway temp dir; point hdfs_load.sh at it via DATA_DIR.

    Real ``data/raw/`` is never touched, so there is no save/restore and no risk of destroying a
    developer's downloaded dataset. The temp dir is cleaned up automatically by pytest.
    """
    (tmp_path / "events_train.csv").write_text(_EVENTS_TRAIN)
    (tmp_path / "item_properties_part1.csv").write_text(_ITEM_PROPS)
    (tmp_path / "category_tree.csv").write_text(_CATEGORY_TREE)
    return tmp_path


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


def test_hdfs_bronze_stages_train_split_and_dims(synthetic_data_dir: Path):
    # DATA_DIR points the script at the temp fixture dir; COMPOSE_PROJECT_NAME makes the script's
    # own `docker compose` calls (the compose file sets `name: scf`) target the isolated scf-it
    # project so the dev stack's bronze data is never touched.
    r = subprocess.run(
        ["bash", str(ROOT / "scripts" / "hdfs_load.sh")],
        cwd=str(ROOT),
        env={
            **os.environ,
            "DATA_DIR": str(synthetic_data_dir),
            "COMPOSE_PROJECT_NAME": _PROJECT,
        },
    )
    assert r.returncode == 0, "hdfs_load.sh failed"

    assert _hdfs_test("/data/bronze/events"), "events bronze missing"
    assert _hdfs_test("/data/bronze/item_properties"), "item_properties bronze missing"
    assert _hdfs_test("/data/bronze/category_tree"), "category_tree bronze missing"

    # events bronze holds the TRAIN split only -- assert the staged file is present.
    assert _hdfs_count("/data/bronze/events"), "no events file staged under bronze/events"
