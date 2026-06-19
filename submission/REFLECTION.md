# Reflection — Day 17 (≤ 200 words)

1. **The flywheel.** `build_preference_pairs` — if prompt→chosen/rejected matching is off, you silently get wrong pairs. Detect via prompt cardinality checks and monitoring the pair-ratio post decontamination.

2. **Decontamination.** Skipping it trains on eval prompts, inflating eval accuracy. The lie surfaces when production prompts underperform vs held-out benchmarks — classic train/eval contamination.

3. **Point-in-time.** In fraud detection, joining a user's total transaction count *as of today* into a past transaction's feature leaks future data. Without `ASOF`, the model learns patterns unavailable at inference.

4. **Graph vs vector.** KG answers "Which warehouse ships widgets?" (2-hop) where vector search fails since no single chunk contains both entities. Vector is overkill for "What is the return policy?" — one chunk suffices, embedding adds latency.