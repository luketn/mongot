---
name: mongot-search-breakdown
description: Generate MongoT Atlas Search load-test breakdown reports and static search breakdown charts from k6 output plus Jaeger traces. Use when asked to rerun the Atlas Search Coco k6 workload, compare MongoT trace segment timings, regenerate load-results/breakdown markdown, or create the presentation-style MongoT search breakdown PNG.
---

# MongoT Search Breakdown

Use this skill to repeat the Atlas Search Coco load test and regenerate the
MongoT search breakdown report/chart under `load-results/breakdown`.

## Quick Start

From the MongoT repo root:

```sh
.codex/skills/mongot-search-breakdown/scripts/run_search_breakdown.sh \
  --coco-dir /Users/luketn/code/personal/atlas-search-coco \
  --vus 25 \
  --duration 5m
```

For the full scenario suite used by the MongoT app-tour trace narrative:

```sh
.codex/skills/mongot-search-breakdown/scripts/run_trace_scenario_breakdowns.sh \
  --coco-dir /Users/luketn/code/personal/atlas-search-coco \
  --vus 25 \
  --duration 5m \
  --mutation-vus 1 \
  --scenarios text,vector,both,text-license,mutations
```

The wrapper uses the skill-local Python environment:

```text
.codex/skills/mongot-search-breakdown/.venv
```

If the environment is missing, the wrapper recreates it and installs
`requirements.txt`.

## Workflow

1. Confirm MongoT, Jaeger, and the Atlas Search Coco app are already running.
2. Run the wrapper above.
3. Inspect the generated markdown path printed at the end.
4. If Grafana is needed, use the same run window from the generated `run-*.env`
   file.

For multi-scenario work:

1. Confirm MongoT is running with `DETAILED_TRACE_SPANS=true`, Jaeger is
   reachable on `http://localhost:16686`, and Atlas Search Coco is reachable on
   `http://localhost:8222`.
2. Run `scripts/run_trace_scenario_breakdowns.sh`.
3. Review `load-results/breakdown/trace-breakdown-summary-<run>.md`.
4. Each scenario has its own k6 log, run env file, Markdown report, and PNG
   chart.

## Script Behavior

`scripts/generate_search_breakdown.py`:

- Runs `k6 run -e K6_VUS=<vus> -e K6_DURATION=<duration> k6.js`.
- Saves the raw k6 log and run metadata.
- Queries Jaeger for `mongodb.CommandService/atlasSearchCoco.image/search`.
- Samples up to 2,000 traces by default.
- Computes median span timings and derived residual buckets.
- Draws the static PNG breakdown with Pillow.
- Writes `load-results/breakdown/search-breakdown-YYYY-MM-DD.md`.

Useful options:

```sh
--skip-k6
--start-epoch 1777102929
--end-epoch 1777103249
--k6-log load-results/breakdown/k6-20260425-174209.log
--trace-limit 2000
--operation mongodb.CommandService/atlasSearchCoco.image/search
--output-dir load-results/breakdown
--image-path test.png
```

Use `--skip-k6` when regenerating a chart from an existing run window.

`scripts/generate_trace_scenario_breakdowns.py`:

- Runs the text, vector, combined, text-with-license, and mutation scenarios.
- Search scenarios use Atlas Search Coco `k6.js`.
- Mutation scenarios use Atlas Search Coco `k6-mutations.js`, which inserts
  synthetic images and deletes those generated ids through the Coco API.
- Queries Jaeger immediately after each run to avoid losing traces to the
  in-memory Jaeger cap.
- Writes per-scenario reports named
  `trace-breakdown-<scenario>-<run>.md` and charts named
  `trace-breakdown-<scenario>-<run>.png`.
- Writes a summary index named `trace-breakdown-summary-<run>.md`.

Useful options:

```sh
--scenarios text,vector,both,text-license,mutations
--vus 25
--duration 5m
--mutation-vus 1
--mutation-duration 2m
--trace-limit 2000
--base-url http://localhost:8222
--jaeger-url http://localhost:16686
```

Keep mutation VUs low unless the Coco API has been changed to allocate image
ids atomically. `/image/add` currently calls `nextImageId()` before inserting,
so concurrent inserts can race and fail with duplicate ids.
