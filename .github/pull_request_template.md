# Pull Request

**Layer / owner:** <!-- infra(Nagy) | ingestion(Hatem) | batch(Emad) | streaming(Nashat) | automation(Ziad) -->
**Plan task:** <!-- e.g. emad-plan Task 4 -->

## What & why
<!-- One or two sentences. -->

## Contract impact
- [ ] No change to `contracts/` (topics/Avro/Redis keys/HDFS paths/model signatures)
- [ ] Changes a contract — **adjacent owners tagged for review** (see master-plan §1)

## Checklist (Definition of Done)
- [ ] TDD: failing test → minimal code → passing test
- [ ] `ruff` + `black` + `mypy` clean
- [ ] Tests pass locally (`make test`)
- [ ] Metric emitted where relevant (`libs.scf_common.observability`)
- [ ] Zero-cost preserved (no paid API/SDK; `scripts/check_no_paid_deps.py` passes)
- [ ] Plan checkbox ticked
