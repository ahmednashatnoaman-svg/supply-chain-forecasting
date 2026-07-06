# Financial Model & Business Case

Quantifies the value the platform creates, using the e-commerce financial framework (traffic ×
conversion × AOV × frequency). This justifies the project and powers the executive dashboard's ROI view.
Owner: **Ziad** (business-facing) with **Nashat** (pricing) consulted. **Project infra cost = $0** (see
[cost-and-licensing](cost-and-licensing.md)), so ROI ≈ pure upside.

## 1. Revenue drivers (e-commerce model)

```
Net revenue = Traffic × Conversion × AOV × Purchase frequency − COGS
Gross margin target: 40–60% (typical e-commerce)
```

The platform improves three levers:
1. **AOV / margin** ↑ via dynamic surge pricing (capture willingness-to-pay on viral items).
2. **Conversion** ↑ via fewer stock-outs (demand forecast → proactive reorder before shelves empty).
3. **Holding cost** ↓ via less overstock (forecast-driven inventory, fewer markdowns).

## 2. Illustrative baseline (assumption-driven — replace with real data)

| Input | Value |
|---|---|
| Monthly orders | 100,000 |
| AOV | $45 |
| Gross margin | 50% |
| Monthly gross profit (baseline) | $2,250,000 |
| Stock-out lost-sales rate | 4% |
| Overstock markdown drag | 3% of margin |

## 3. Three-scenario impact (P10 / P50 / P90)

Impact = incremental gross profit/month from the three levers. Conservative assumptions per the
financial-modeling skill (never over-optimistic).

| Lever | P10 (conservative) | P50 (base) | P90 (optimistic) |
|---|---|---|---|
| Surge pricing margin uplift | +0.8% | +2.0% | +3.5% |
| Stock-out recovery (of the 4% lost) | 15% recovered | 35% recovered | 55% recovered |
| Overstock/markdown reduction | +0.5% margin | +1.2% margin | +2.0% margin |
| **Est. incremental gross profit/mo** | **≈ +$60k** | **≈ +$150k** | **≈ +$260k** |

> Method: uplift% × baseline gross profit + recovery% × (stock-out rate × baseline revenue × margin) +
> markdown reduction% × baseline gross profit. Numbers are illustrative; the dashboard recomputes them
> live from Redis + streaming actuals.

## 4. ROI

Because the stack is **100% free/open-source and self-hosted**, the only real cost is engineering time
(5 people building it). There is **no per-unit infra or API cost** to erode the upside — the incremental
gross profit above is effectively the return. Payback is measured in *days of operation* once live.

## 5. Unit-economics guardrails (tie to pricing strategy)

- Every autonomous price move respects `min_margin_pct` — the model can never trade margin for volume
  below the floor.
- Reorder automation is bounded by `INVENTORY_REORDER_THRESHOLD` + target stock to avoid overstock swing.
- Optional cash-flow check before large reorders via `automation/finance/plaid_reconciliation.py`
  (Plaid **Sandbox**, free) — verify supplier-payment funds before dispatch.

## 6. Dashboard hook

The Streamlit "command center" surfaces a live ROI tile: baseline gross profit vs. realized incremental
profit (from `automated_pricing_updates` + recovered stock-outs), using the P10/P50/P90 bands above as
reference lines.
