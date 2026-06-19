# Bonus Design: Flywheel for Vietnamese E-Commerce CSKH Chatbot

## Problem & Constraints

A mid-size Vietnamese e-commerce platform (200K orders/month, 15K daily chat conversations) wants to build an **agent-data flywheel** for its customer-service chatbot. The chatbot currently runs on a static FAQ + retrieval pipeline with ~72% first-contact resolution. The goal: use production traces to generate eval sets and preference data for fine-tuning, improving the bot monthly.

**Real constraints:**
- **Language:** Vietnamese (full diacritics, regional variants like "gadget" vs "đồ điện tử", code-switching English/Vietnamese)
- **Data volume:** ~500 conversations/day × 30 days = 15K traces/month, each with 3-8 spans
- **Compliance:** PDPL (Law 91/2025) requires explicit consent for using customer conversation data to train models; users can opt out and request deletion
- **Cost:** No dedicated ML infra team; one data engineer shared across products
- **Existing stack:** PostgreSQL order DB, Elasticsearch for product search, chatbot logs in JSON Lines on S3-compatible object storage

---

## 1. Source & Shape — Data Drift Is the Silent Killer

**Decision:** Schema-on-read with a **contract version** field in every trace, validated by Pandera (same pattern as lab §3).

**Why:** Chatbot traces evolve as the product team adds new intents ("return gadget" vs "cancel order"). A fixed Avro/Protobuf schema would break deployments. Instead, each trace carries `schema_version: "v2"` and a set of optional `attributes.*` fields. Pandera validates a *minimum* required set; extra fields are preserved but flagged.

**Tradeoff:** Schema-on-read shifts breakage detection from compile-time to runtime. A dashboard alert (`pipeline.monitor.quarantine_spike`) triggers when >5% of daily traces fail validation. The alternative — Avro with compatibility modes — was rejected because it requires a schema registry and adds latency to trace ingestion (the chatbot team deploys 3× weekly; registry updates couldn't keep up).

**Rejected alternative:** Storing traces as raw JSON in a MongoDB collection. Reason: without a typed layer, downstream consumers (eval set builder, DPO miner) would each implement parsing differently, causing silent inconsistencies. The Bronze → typed Silver pattern from the lab proved necessary here too.

---

## 2. Batch vs Streaming — Nightly Batch Wins (For Now)

**Decision:** Daily batch. Traces land in object storage every hour (micro-batch), but the full flywheel (flatten → eval set → DPO pairs → PIT features) runs once at 02:00.

**Why:** The chatbot doesn't need same-day fine-tuning. A 24-hour window allows human reviewers to label ambiguous turns (see §5) and lets the PDPL consent check run: traces from users who opted out in the last 24h are excluded before the flywheel starts.

**When this breaks:** At 5× volume (~75K conversations/day), the hourly micro-batch will still be fine, but the flywheel DAG may exceed the 4-hour SLA. Mitigation: incremental processing with watermark (only process traces newer than last successful run's watermark).

**Rejected alternative:** Redpanda/Kafka streaming (like the Docker bonus). Rejected because the team has no Kafka ops expertise; object storage + a cron DAG is debuggable by a single engineer. Latency of 24h is acceptable for the use case.

---

## 3. Quality & Contract — Three Gates Before Data Touches a Model

**Decision:** Three gates, each with separate quarantine:

1. **Schema gate** (Pandera, same as lab): valid trace structure? Missing `span_id`, `parent_id`, `status`? → quarantine.
2. **Consent gate** (custom): does the user have a valid PDPL consent record in PostgreSQL at trace time? → discard, no quarantine (data that shouldn't exist).
3. **Semantic gate** (LLM-as-a-judge, lightweight): for error traces, is the `output` actually wrong? Some "error" traces contain correct answers but are mislabeled due to a tool bug. A cheap model (Gemini 2.0 Flash, $0.15/1M tok) scores these; low-confidence ones go to human review queue.

**Tradeoff:** The LLM-as-a-judge gate adds latency (~3s per trace) and cost (~$2.25/day for 15K traces). But without it, we'd train on false-error pairs (e.g., correct answers labeled "ToolError"), poisoning the DPO dataset. The human review queue is the bottleneck: it caps throughput at ~200 traces/day. We accept this for the first 3 months and plan to replace it with a learned classifier trained on reviewed judgments.

**Rejected alternative:** Use only exact-rule gates (schema + consent). This would let through ~8% of false-error traces (measured in a pilot), making the DPO pairs ~8% garbage — enough to degrade fine-tune quality silently.

---

## 4. Train/Serve Parity — Two Leaks to Watch

**Decision:** Three levers for point-in-time correctness:

1. **Feature store with `ASOF` semantics** (DuckDB in dev, plan to migrate to Redis+Postgres for prod): features are materialized with `valid_from`/`valid_to` timestamps; joins use `ASOF` (lab §11).
2. **Feature logging at inference time:** each chatbot response logs the exact feature vector used. During training, we join on `(user_id, event_id)` rather than recomputing features — eliminating recency bias.
3. **Backtest with cutoff dates:** before any training run, we simulate "what did the features look like at time T?" for 10 random cutoff dates. If the `ASOF` result differs from the logged features by >1%, alert.

**Known leak #1:** `user_total_orders` computed with a `GROUP BY` without a timestamp filter. Fixed by making it `SUM(orders) WHERE event_ts <= current_event_ts`.

**Known leak #2:** Product embeddings updated daily; a conversation at 09:00 might use today's embedding that was computed at 02:00. This is a *temporal misalignment* but not a leak (the embedding is timestamped). Documented and accepted.

**Rejected alternative:** Recompute all features from scratch for each training dataset (like a fully deterministic backfill). Rejected because it makes iterative feature engineering prohibitively slow (15K traces × 20 features × 10 iterations = 3M recomputes).

---

## 5. Decontamination — Vietnamese Adds a Twist

**Decision:** Two-level decontamination: exact-match (like the lab) + **fuzzy n-gram** (character 13-grams, Jaccard similarity ≥0.85).

**Why:** Vietnamese prompts are often paraphrased. "Có thể trả lại widget đã mua 10 ngày trước không?" and "Widget mua 10 ngày trước có trả lại được không?" contain zero identical words but are semantically identical. Exact-match decontamination would miss them, silently leaking eval prompts into training.

**Vietnamese-specific challenge:** Vietnamese word segmentation (e.g., "trả_lại" vs "trả lại") creates noise in n-gram matching. Solution: normalize by removing spaces within known compound words using pyvi tokenizer before building n-grams.

**Cost:** Character 13-gram decontamination on 15K traces takes ~4 minutes. The accepted tradeoff: we might over-decontaminate (drop a genuinely novel prompt that happens to share n-grams with an eval prompt). Monitoring: track the "false positive rate" by having a human label a random 1% sample of dropped pairs quarterly.

**Rejected alternative:** Embedding-based decontamination (reuse `embed.py` from lab). Rejected because embeddings are sensitive to the embedding model's training data; a model fine-tuned on customer-service data might embed similar prompts close together even if they ask different things, causing over-decontamination.

---

## Architecture Sketch

```
                          ┌──────────────────────┐
                          │  Chatbot Trace Logs   │
                          │ (JSON Lines, S3)      │
                          └──────────┬───────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │  Bronze (raw, typed)  │
                          │  DuckDB :memory:      │  ← schema gate
                          └──────────┬───────────┘
                                     │
                   ┌─────────────────┼─────────────────┐
                   ▼                 ▼                  ▼
          ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
          │ Consent Gate  │  │ Flatten spans │  │ Semantic Gate │
          │ (PostgreSQL)  │  │ → Bronze_flat │  │ (LLM judge)   │
          └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
                 │                 │                  │
                 ▼                 ▼                  ▼
          ┌──────────────────────────────────────────────┐
          │  Silver: Clean, typed, deduped spans         │
          └──────┬───────────────────────────────────────┘
                 │
         ┌───────┴───────────┬─────────────────┐
         ▼                   ▼                  ▼
  ┌─────────────┐   ┌──────────────┐   ┌──────────────┐
  │ Eval Set    │   │ DPO Pairs    │   │ PIT Features  │
  │ (holdout)   │   │ (chosen/     │   │ (ASOF join)   │
  │             │   │  rejected)   │   │              │
  └──────┬──────┘   └──────┬───────┘   └──────┬───────┘
         │                 │                  │
         └─────────────────┼──────────────────┘
                           ▼
                  ┌────────────────┐
                  │ Decontaminate  │
                  │ (exact + 13-gram)│
                  └───────┬────────┘
                          ▼
                  ┌────────────────┐
                  │ Train Dataset  │ → Day 22 SFT/DPO
                  └────────────────┘
```

---

## Cost & Vietnam Context

**Monthly cost estimate at 15K traces:**
- Object storage: ~$3 (200MB/month)
- Compute (EC2 t3.medium, reserved): ~$25
- LLM judge API: ~$70 (Gemini Flash)
- Human review (part-time): ~$200
- **Total: ~$300/month**

**80% cost:** LLM judge + human review. To cut, we could replace the LLM judge with a fine-tuned PhoBERT classifier after we accumulate 3 months of reviewed data. But that's a classic bootstrap problem — you need the judge to generate the data to replace the judge.

**Vietnam-specific decisions:**
- PDPL compliance required a consent gate *before* any data touches the training pipeline, not after. This is reversed from US/EU patterns where consent is checked at serving time.
- Object storage is S3-compatible (Viettel Cloud / VNG Cloud) rather than AWS S3 — same API, but egress costs are 2× higher ($0.09/GB vs $0.05/GB). We minimize egress by running the flywheel inside the same cloud region.
- The human reviewer must be Vietnamese-speaking, adding hiring constraints. We budget for a part-time contractor rather than a full FTE.