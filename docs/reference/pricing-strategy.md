# Pricing Strategy (algorithmic dynamic pricing)

Applies value-based pricing + price-perception psychology to the real-time engine. This is the
**strategy**; the math lives in `streaming/pricing/formula.py` and the presentation in
`streaming/pricing/psychology.py`. Owner: **Nashat** (pricing), business input from **Ziad**.

## 1. Value-based frame (not cost-plus)

```
Customer perceived value  (demand signal: velocity ≫ baseline during a surge)
────────────────────────
Our dynamic price         (base_price × (1 + uplift), uplift damped by elasticity)
────────────────────────
Next best alternative      (competitor / substitute SKU in same community)
────────────────────────
Cost to serve             (unit cost — a FLOOR, never the pricing basis)
```

Rules encoded in the engine:
- **Price above the floor, below perceived value.** `min_margin_pct` guarantees the floor; `max_uplift_pct`
  caps how far we push toward perceived value so we leave customer surplus (protects trust/retention).
- **Cost is a floor, not a basis.** We never compute price from cost; we compute from demand + elasticity.
- **Elasticity damping = cannibalization guard.** Highly connected SKUs (`graph:elasticity`, `graph:community`)
  move *less*, so raising one price doesn't tank a linked high-margin accessory (cross-elasticity).

## 2. The value metric

For dynamic retail pricing the value metric is **demand velocity relative to the statistical baseline**
(`velocity / baseline`). It scales price with realized customer value (willingness-to-pay revealed by a
surge) and is hard to game (baseline is model-derived from history, not self-reported).

## 3. Guardrails (safe price-change rules)

| Guardrail | Setting | Purpose |
|---|---|---|
| Max uplift | `PRICE_MAX_UPLIFT_PCT` (0.25) | Never raise > 25% in a window — avoids gouging / backlash |
| Min margin | `PRICE_MIN_MARGIN_PCT` (0.10) | Never sell below margin floor |
| Surge threshold | `SURGE_VELOCITY_THRESHOLD` (2.0) | Only move price when velocity ≥ 2× baseline AND LSTM confirms surge |
| Elasticity damp | `PRICE_ELASTICITY_COEFF` (0.35) | Connected SKUs move less |

These mirror the pricing skill's "safe methods": we change price on *new demand conditions*, not via
blind A/B shocks on the same customer.

## 4. Price presentation psychology (honest only)

Implemented in `psychology.py`, governed by hard ethical rules:
- **Charm endings** (left-digit effect): display 120.99 rather than 121.00 — but **never above** the
  computed price (charm rounding only goes down/equal → preserves trust).
- **Honest anchoring**: the only anchor shown is the *real* catalog `base_price`, and only when the price
  genuinely moved. **No fabricated "was" prices, no fake countdowns.**
- **Quality-signal protection**: `min_display_floor_pct` stops presentation from cheapening a premium SKU.

## 5. Ethics line

We surface a *real* value choice (demand-driven price) transparently. We never engineer confusion about
what the customer pays. Surge pricing is disclosed via the honest anchor + reason string on every
`automated_pricing_updates` event.
