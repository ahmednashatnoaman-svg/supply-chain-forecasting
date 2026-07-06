---
name: scf-onboard
description: Use when a team member starts working in this repo and wants to get oriented — which layer they own, what their next open task is, and the conventions to follow. Trigger phrases — "onboard me", "what should I work on", "get me started", "/scf-onboard".
argument-hint: "[your-name: nagy|hatem|emad|nashat|ziad]"
disable-model-invocation: false
---

# Supply Chain Forecasting — Team Onboarding

Orient a team member (Nagy, Hatem, Emad, Nashat, or Ziad) in under a minute: their layer, their next
open task, and the rules that keep 5 people from colliding.

## Step 1 — Identify the member

If `$ARGUMENTS` names one of `nagy|hatem|emad|nashat|ziad`, use that. Otherwise infer from
`git config user.name` / `user.email` matched against `.github/CODEOWNERS`, and if still ambiguous,
ask the user directly which of the 5 they are.

## Step 2 — Show their ownership

| Member | Layer | Directory | Plan doc |
|---|---|---|---|
| Nashat | Speed: streaming + LSTM + pricing; shared contracts/CI | `streaming/`, `libs/` | `docs/plans/nashat-plan.md` |
| Emad | Batch: ETL + MLlib + GraphFrames + Airflow | `batch/` | `docs/plans/emad-plan.md` |
| Nagy | Infrastructure: Docker/HDFS/Spark/Kafka/Redis + monitoring | `infra/` | `docs/plans/nagy-plan.md` |
| Hatem | Ingestion & simulation: Kafka topics, traffic generator | `ingestion/` | `docs/plans/hatem-plan.md` |
| Ziad | Automation & serving: n8n, Streamlit dashboard | `automation/` | `docs/plans/ziad-plan.md` |

Read their plan doc's header (Goal/Architecture/Tech Stack) aloud to orient them, then run:

```bash
gh issue list --label "owner:<their-name>" --state open
```

Pick the **lowest-numbered task** in their plan doc that's still open — tasks build on each other
(e.g., Emad's Task 2 features depend on Task 1's clean_events), so work top-to-bottom within a plan
unless a dependency is already satisfied by someone else's merged work.

## Step 3 — Remind them of the loop (full detail: `docs/runbooks/git-workflow.md`)

1. `git checkout main && git pull --ff-only`
2. `git checkout -b <github-username>/<layer>-task-<N>-<slug>`
3. Work the task exactly as written in their plan doc: failing test → implement → passing test → commit
4. Push, open a PR with `Closes #<issue-number>`
5. If the change touches `contracts/`, `libs/`, `.github/`, `pyproject.toml`, or
   `infra/docker/docker-compose.yml` — tag the adjacent owner from `.github/CODEOWNERS` explicitly
   in the PR description (these are shared, high-conflict paths)
6. Wait for CI green (`lint-type`, `unit`, `no-paid-deps`, `integration`) — no mandatory human
   approval is required to merge, but reviews are welcome
7. Squash-merge (branch auto-deletes)

## Step 4 — Non-negotiable project rules

- **Zero-cost only.** No paid APIs/models/cloud — see `docs/reference/cost-and-licensing.md`. A CI
  guard (`scripts/check_no_paid_deps.py`) fails the build on a denylisted paid SDK.
- **Never hard-code a topic name, Redis key, or HDFS path.** Import from
  `libs/scf_common.contracts` (`Topics`, `RedisKeys`, `HdfsPaths`) — see `docs/architecture/data-contracts.md`.
- **TDD, no exceptions.** Every task in every plan doc is failing-test-first. A PR with no new/updated
  test for its behavior change should not merge.
- **Stay in your directory.** Cross-layer changes are the exception, not the norm — see the
  "Avoiding conflicts" section of `docs/runbooks/git-workflow.md`.

## Step 5 — Point to deeper references if their task needs them

- Building/tuning Kafka producer or consumer code → `docs/reference/kafka-playbook.md`
- Spark job feels slow, or touches the GraphFrames join → `docs/reference/spark-playbook.md`
- Training or promoting a model (MLlib forecast or LSTM) → `docs/reference/mlops-playbook.md`
- Anything about the pricing formula or price display → `docs/reference/pricing-strategy.md`

## Output

End with a one-paragraph, name-addressed summary: their layer, the exact next issue number + title,
the branch name they should create, and which playbook (if any) applies to that specific task.
