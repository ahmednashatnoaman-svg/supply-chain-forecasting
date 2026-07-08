"""Unit test: the n8n reorder workflow is valid JSON with a webhook trigger and an email/log node
(Ziad plan Task 4)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

WORKFLOW_PATH = Path(__file__).resolve().parents[2] / "automation" / "n8n" / "reorder_workflow.json"


@pytest.fixture(scope="module")
def workflow():
    return json.loads(WORKFLOW_PATH.read_text())


def test_workflow_is_valid_json_with_nodes_and_connections(workflow):
    assert "nodes" in workflow and isinstance(workflow["nodes"], list)
    assert "connections" in workflow


def test_workflow_has_a_webhook_trigger_node(workflow):
    webhook_nodes = [n for n in workflow["nodes"] if n["type"] == "n8n-nodes-base.webhook"]
    assert len(webhook_nodes) == 1
    assert webhook_nodes[0]["parameters"]["path"] == "reorder"
    assert webhook_nodes[0]["parameters"]["httpMethod"] == "POST"


def test_workflow_has_an_email_or_log_node(workflow):
    email_or_log_nodes = [n for n in workflow["nodes"] if "email" in n["name"].lower()]
    assert len(email_or_log_nodes) == 1


def test_workflow_nodes_are_fully_connected(workflow):
    """Every node is reachable from the Webhook trigger — no orphan nodes.

    Originally asserted "exactly one terminal node", which held for the workflow's first,
    purely-linear shape. The security-audit follow-up added a branching IF node (shared-secret
    check), a normal n8n pattern that legitimately produces more than one terminal/leaf node (the
    happy path and the "Unauthorized" rejection path both end without an outgoing connection).
    Reachability from the trigger is the invariant that actually matters -- an orphan node
    (unreachable, dead in the workflow) would still be caught by this.
    """
    node_names = {n["name"] for n in workflow["nodes"]}
    webhook_name = next(
        n["name"] for n in workflow["nodes"] if n["type"] == "n8n-nodes-base.webhook"
    )

    reachable = {webhook_name}
    frontier = [webhook_name]
    while frontier:
        current = frontier.pop()
        for branch in workflow["connections"].get(current, {}).get("main", []):
            for edge in branch:
                target = edge["node"]
                if target not in reachable:
                    reachable.add(target)
                    frontier.append(target)

    assert reachable == node_names, f"unreachable/orphan nodes: {node_names - reachable}"
