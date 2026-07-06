# Runbook — Git & PR Workflow

How the 5-person team ships changes without stepping on each other. This governs `main` and every
branch; see [`master-plan.md`](../master-plan.md) for who owns what.

## Branch naming

`<owner>/<layer>-task-<N>-<short-slug>`, one branch per plan task (small, reviewable, one clear
deliverable — matches the task sizing in `docs/plans/*.md`).

```
mohamed-nagy11/infra-task-1-compose-stack
3omdawy11/batch-task-4-mllib-forecast
ahmednashatnoaman-svg/streaming-task-5-pricing-stream
```

## The loop (per task)

1. Pick an open [GitHub Issue](https://github.com/ahmednashatnoaman-svg/supply-chain-forecasting/issues)
   labeled `owner:<you>` — it links straight to your plan task.
2. Branch from an up-to-date `main`.
3. Work the task exactly as written (failing test → implement → passing test → commit). Small commits.
4. Push and open a PR using the template. Reference `Closes #<issue-number>`.
5. If the PR touches `contracts/`, `libs/`, `.github/`, or `pyproject.toml`/`docker-compose.yml` —
   these are **shared, high-conflict paths** — tag the adjacent owner from `.github/CODEOWNERS`
   explicitly in the PR description, not just via auto-review.
6. CI must go green (`ci-lint-type`, `ci-unit`, `ci-zero-cost-guard`, `ci-integration`).
7. **At least 1 approving review required** (branch protection enforces this — see below).
8. Squash-merge. Branch auto-deletes.

## Branch protection on `main` (configured)

- No direct pushes — every change goes through a PR.
- Required status checks: `lint-type`, `unit`, `no-paid-deps`, `integration`.
- **At least 1 approving review** required before merge (from any collaborator with write access —
  not restricted to CODEOWNERS, so a busy reviewer never fully blocks the team).
- Linear history enforced (squash-merge only — see repo settings below).
- Force-pushes and branch deletion blocked on `main`.

## Avoiding conflicts (harmony rules)

- **Stay in your directory.** Each layer owns its own top-level folder
  (`infra/`, `ingestion/`, `batch/`, `streaming/`, `automation/`) — normal task work never touches
  another owner's folder.
- **Shared paths need extra care:** `contracts/`, `libs/scf_common/`, `.github/`, `pyproject.toml`,
  `infra/docker/docker-compose.yml`, `Makefile`. Keep PRs to these small and merge them fast so
  branches don't diverge for long.
- **Rebase, don't let branches go stale.** If `main` moves while your branch is open,
  `git fetch origin && git rebase origin/main` before pushing again — don't merge `main` into your
  branch (keeps history linear for squash-merge).
- **Milestone order matters.** Per `master-plan.md`'s dependency graph: Nagy's infra (M0) unblocks
  everyone; Hatem's ingestion contracts (M0/M1) unblock Nashat & Emad's Kafka reads; Emad's Redis
  publish (M2) is mocked by `scripts/seed_redis_stub.py` so Nashat is never idle waiting on it.

## Task board

All 29 plan tasks are tracked as
[GitHub Issues](https://github.com/ahmednashatnoaman-svg/supply-chain-forecasting/issues), labeled
`layer:<name>`, `owner:<name>`, and `plan-task`. Issues already closed were verified complete and
passing in CI at scaffold time (see the issue's closing comment for which test proves it).
