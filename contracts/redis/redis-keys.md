# Redis Key Schema (serving-layer contract)

Single source of truth for the serving cache. Access **only** via
`libs/scf_common.contracts.RedisKeys` — never build key strings by hand.

| Key pattern | Value type | TTL | Written by | Read by |
|---|---|---|---|---|
| `forecast:{sku}` | float as string | none (refreshed nightly) | Emad (batch) | Nashat, Ziad |
| `graph:elasticity:{sku}` | JSON: `[{"related": <sku>, "weight": <float>}]` | none | Emad | Nashat |
| `graph:community:{sku}` | int as string | none | Emad | Nashat |
| `price:current:{sku}` | float as string | 1h | Nashat (speed) | Ziad |
| `inventory:{sku}` | int as string | none | Hatem / Nashat | Nashat, Ziad |
| `velocity:{sku}:{window}` | float as string | 5m | Nashat (speed) | Ziad |

`{sku}` is the numeric `item_id`. `{window}` is the window label, e.g. `60s`.

## Accessor mapping (`RedisKeys`)

```
RedisKeys.forecast("10")        -> "forecast:10"
RedisKeys.elasticity("10")      -> "graph:elasticity:10"
RedisKeys.community("10")       -> "graph:community:10"
RedisKeys.price("10")           -> "price:current:10"
RedisKeys.inventory("10")       -> "inventory:10"
RedisKeys.velocity("10", "60s") -> "velocity:10:60s"
```

## Consistency rules
- Batch writes are **atomic per SKU** (Redis pipeline / MULTI) so the stream never reads a half-updated
  forecast+graph pair.
- The stream is the **only** writer of `price:current:*` and `velocity:*`. Batch is the only writer of
  `forecast:*` and `graph:*`. No key has two writers → no race.
