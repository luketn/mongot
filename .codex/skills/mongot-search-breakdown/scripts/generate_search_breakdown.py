#!/usr/bin/env python3
import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def parse_args() -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(
        description="Generate MongoT search load-test breakdown markdown and PNG."
    )
    parser.add_argument("--repo-root", type=Path, default=root)
    parser.add_argument(
        "--coco-dir",
        type=Path,
        default=Path("/Users/luketn/code/personal/atlas-search-coco"),
    )
    parser.add_argument("--output-dir", type=Path, default=root / "load-results/breakdown")
    parser.add_argument("--image-path", type=Path)
    parser.add_argument("--vus", default="25")
    parser.add_argument("--duration", default="5m")
    parser.add_argument("--base-url", default="")
    parser.add_argument("--include-license", default="")
    parser.add_argument("--request-timeout", default="")
    parser.add_argument("--skip-k6", action="store_true")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--start-epoch", type=int, default=0)
    parser.add_argument("--end-epoch", type=int, default=0)
    parser.add_argument("--k6-log", type=Path)
    parser.add_argument("--jaeger-url", default="http://localhost:16686")
    parser.add_argument("--trace-limit", type=int, default=2000)
    parser.add_argument(
        "--operation",
        default="mongodb.CommandService/atlasSearchCoco.image/search",
    )
    parser.add_argument("--save-jaeger-json", action="store_true")
    return parser.parse_args()


def run_k6(args: argparse.Namespace) -> tuple[str, int, int, Path, Path]:
    run_id = args.run_id or datetime.now().strftime("%Y%m%d-%H%M%S")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    log_path = args.output_dir / f"k6-{run_id}.log"
    env_path = args.output_dir / f"run-{run_id}.env"

    command = ["k6", "run", "-e", f"K6_VUS={args.vus}", "-e", f"K6_DURATION={args.duration}"]
    if args.base_url:
        command.extend(["-e", f"BASE_URL={args.base_url}"])
    if args.include_license:
        command.extend(["-e", f"INCLUDE_LICENSE={args.include_license}"])
    if args.request_timeout:
        command.extend(["-e", f"REQUEST_TIMEOUT={args.request_timeout}"])
    command.append("k6.js")

    start_epoch = int(time.time())
    with log_path.open("w") as log_file:
        result = subprocess.run(
            command,
            cwd=args.coco_dir,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            check=False,
        )
    end_epoch = int(time.time())

    env_path.write_text(
        "\n".join(
            [
                f"RUN_ID={run_id}",
                f"START_EPOCH={start_epoch}",
                f"START_ISO={format_epoch(start_epoch)}",
                f"K6_LOG={log_path}",
                f"END_EPOCH={end_epoch}",
                f"END_ISO={format_epoch(end_epoch)}",
                f"STATUS={result.returncode}",
                "",
            ]
        )
    )
    if result.returncode != 0:
        raise SystemExit(f"k6 failed with exit code {result.returncode}; see {log_path}")
    return run_id, start_epoch, end_epoch, log_path, env_path


def format_epoch(epoch: int) -> str:
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S %Z")


def load_existing_run(args: argparse.Namespace) -> tuple[str, int, int, Path, Path | None]:
    if not args.start_epoch or not args.end_epoch or not args.k6_log:
        raise SystemExit("--skip-k6 requires --start-epoch, --end-epoch, and --k6-log")
    run_id = args.run_id or datetime.fromtimestamp(args.start_epoch).strftime("%Y%m%d-%H%M%S")
    return run_id, args.start_epoch, args.end_epoch, args.k6_log, None


def fetch_jaeger_traces(args: argparse.Namespace, start_epoch: int, end_epoch: int) -> list[dict]:
    params = urllib.parse.urlencode(
        {
            "service": "mongot",
            "operation": args.operation,
            "start": start_epoch * 1_000_000,
            "end": end_epoch * 1_000_000,
            "limit": args.trace_limit,
        }
    )
    url = f"{args.jaeger_url.rstrip('/')}/api/traces?{params}"
    with urllib.request.urlopen(url, timeout=180) as response:
        data = json.load(response)
    traces = data.get("data", [])
    if args.save_jaeger_json:
        path = args.output_dir / f"jaeger-traces-{start_epoch}.json"
        path.write_text(json.dumps(data))
    if not traces:
        raise SystemExit(f"No Jaeger traces returned for operation {args.operation}")
    return traces


def line_value(text: str, key: str) -> str:
    match = re.search(r"^\s*" + re.escape(key) + r"\.*:\s*(.*)$", text, re.M)
    return match.group(1).strip() if match else ""


def metric_number(line: str, key: str) -> str:
    match = re.search(re.escape(key) + r"=([0-9.]+)", line)
    return match.group(1) if match else ""


def median(values: list[float]) -> float:
    vals = [value for value in values if value is not None]
    return statistics.median(vals) if vals else 0.0


def fmt_time_us(micros: float) -> str:
    if micros >= 1000:
        return f"{micros / 1000:.3f} ms".rstrip("0").rstrip(".")
    if float(micros).is_integer():
        return f"{int(micros)} us"
    return f"{micros:.1f} us"


def pct(value: float, denominator: float) -> str:
    if not denominator:
        return "0.00%"
    return f"{(value / denominator) * 100:.2f}%"


def span_maps(trace: dict) -> tuple[dict[str, list[float]], dict[str, list[float]], dict[str, list[float]]]:
    command_spans = [
        span for span in trace["spans"] if span["operationName"] == "mongot.search.command"
    ]
    command_end = None
    if command_spans:
        command_end = command_spans[0]["startTime"] + command_spans[0]["duration"]

    all_map: dict[str, list[float]] = {}
    initial_map: dict[str, list[float]] = {}
    later_map: dict[str, list[float]] = {}
    for span in trace["spans"]:
        name = span["operationName"]
        duration = span["duration"]
        all_map.setdefault(name, []).append(duration)
        target = initial_map if command_end is not None and span["startTime"] <= command_end else later_map
        target.setdefault(name, []).append(duration)
    return all_map, initial_map, later_map


def sum_ops(span_map: dict[str, list[float]], operations: list[str]) -> float:
    return sum(sum(span_map.get(operation, [])) for operation in operations)


def operation_sets() -> dict[str, list[str]]:
    return {
        "root": ["mongodb.CommandService/atlasSearchCoco.image/search"],
        "command": ["mongot.search.command"],
        "query context": ["mongot.search.prepare_query_context"],
        "parse BSON": ["mongot.search.parse_query_bson", "mongot.search.parse_query"],
        "index lookup": ["mongot.search.lookup_index_catalog", "mongot.search.resolve_index"],
        "cursor options": ["mongot.search.parse_cursor_options"],
        "validate": ["mongot.search.validate_query_options", "mongot.search.validate_query"],
        "response mode": ["mongot.search.prepare_response_mode"],
        "build first batch": ["mongot.search.build_first_batch"],
        "create cursor": ["mongot.search.create_and_register_cursor", "mongot.search.create_cursor"],
        "first batch": ["mongot.search.load_first_cursor_batch"],
        "response doc": ["mongot.search.prepare_response_document"],
        "encode BSON": ["mongot.search.encode_response_bson", "mongot.search.serialize_batch"],
        "cursor setup": [
            "mongot.cursor.select_index_manager",
            "mongot.cursor.choose_batch_size",
            "mongot.cursor.instantiate_cursor",
            "mongot.cursor.register_active_cursor",
            "mongot.cursor.register_index_mapping",
        ],
        "reader query": [
            "mongot.lucene.prepare_search_reader_query",
            "mongot.lucene.search_index_reader.query",
        ],
        "Lucene open searcher": ["mongot.lucene.open_index_searcher"],
        "build query": ["mongot.lucene.build_query", "mongot.lucene.create_search_query"],
        "Lucene highlights": ["mongot.lucene.prepare_highlights", "mongot.lucene.prepare_highlighter"],
        "score details": ["mongot.lucene.prepare_score_details"],
        "Lucene page/sort context": ["mongot.lucene.prepare_pagination_sort_context"],
        "Lucene build sort": ["mongot.lucene.build_sort", "mongot.lucene.create_sort"],
        "search manager": ["mongot.lucene.create_search_manager"],
        "Lucene collect hits": [
            "mongot.lucene.collect_initial_top_docs",
            "mongot.lucene.initial_top_docs",
        ],
        "result producer": ["mongot.lucene.create_result_producer"],
        "Lucene advance batch": ["mongot.cursor.advance_batch_producer"],
        "materialize BSON": [
            "mongot.lucene.materialize_bson_documents",
            "mongot.lucene.materialize_results",
        ],
        "Lucene collect more": [
            "mongot.lucene.collect_more_top_docs",
            "mongot.lucene.get_more_top_docs",
        ],
    }


def calculate_metrics(traces: list[dict], operation: str) -> dict:
    ops = operation_sets()
    ops["root"] = [operation]
    values = {name: [] for name in ops}
    computed = {
        name: []
        for name in [
            "stream lifecycle",
            "cursor orchestration",
            "reader orchestration",
            "batch orchestration",
            "command orchestration",
        ]
    }
    later_values = {name: [] for name in ["Lucene collect more", "materialize BSON", "Lucene advance batch"]}

    for trace in traces:
        all_map, initial, later = span_maps(trace)
        for name, operations in ops.items():
            source = all_map if name in ["root", "command"] else initial
            values[name].append(sum_ops(source, operations))

        root_us = sum_ops(all_map, ops["root"])
        command_us = sum_ops(all_map, ops["command"])
        computed["stream lifecycle"].append(max(0, root_us - command_us))

        cursor_create = sum_ops(initial, ops["create cursor"])
        cursor_setup = sum_ops(initial, ops["cursor setup"])
        reader_query = sum_ops(initial, ops["reader query"])
        computed["cursor orchestration"].append(max(0, cursor_create - cursor_setup - reader_query))

        reader_children = sum_ops(
            initial,
            ops["Lucene open searcher"]
            + ops["build query"]
            + ops["Lucene highlights"]
            + ops["score details"]
            + ops["Lucene page/sort context"]
            + ops["Lucene build sort"]
            + ops["search manager"]
            + ops["Lucene collect hits"]
            + ops["result producer"],
        )
        computed["reader orchestration"].append(max(0, reader_query - reader_children))

        first_batch = sum_ops(initial, ops["first batch"])
        initial_advance = sum_ops(initial, ops["Lucene advance batch"])
        initial_materialize = sum_ops(initial, ops["materialize BSON"])
        computed["batch orchestration"].append(
            max(0, first_batch - initial_advance - initial_materialize)
        )

        top_level = sum_ops(
            initial,
            ops["query context"]
            + ops["parse BSON"]
            + ops["index lookup"]
            + ops["cursor options"]
            + ops["validate"]
            + ops["response mode"]
            + ops["build first batch"]
            + ops["response doc"]
            + ops["encode BSON"],
        )
        computed["command orchestration"].append(max(0, command_us - top_level))

        for name in later_values:
            later_values[name].append(sum_ops(later, ops[name]))

    return {"values": values, "computed": computed, "later": later_values}


def sample_payload_info(traces: list[dict]) -> dict:
    info = {
        "trace_id": "",
        "operation_id": "",
        "lucene_query": "",
        "lucene_query_class": "",
        "result_sample_count": "",
        "reader_docs": "",
        "reader_segments": "",
        "query_type": "",
        "index_name": "",
    }
    for trace in traces:
        info["trace_id"] = trace.get("traceID", "")
        for span in trace["spans"]:
            tags = {tag["key"]: tag.get("value") for tag in span.get("tags", [])}
            name = span["operationName"]
            if name == "mongot.search.command":
                info["operation_id"] = tags.get("mongot.operation.id", info["operation_id"])
                info["query_type"] = tags.get("mongot.search.query.type", info["query_type"])
                info["index_name"] = tags.get("mongot.search.index.name", info["index_name"])
            if name == "mongot.lucene.build_query":
                info["lucene_query"] = tags.get("mongot.lucene.query", info["lucene_query"])
                info["lucene_query_class"] = tags.get(
                    "mongot.lucene.query.class", info["lucene_query_class"]
                )
            if name == "mongot.lucene.materialize_bson_documents":
                info["result_sample_count"] = tags.get(
                    "mongot.result.sample.count", info["result_sample_count"]
                )
            if name == "mongot.lucene.open_index_searcher":
                info["reader_docs"] = tags.get("mongot.lucene.reader.num_docs", info["reader_docs"])
                info["reader_segments"] = tags.get(
                    "mongot.lucene.reader.segment_count", info["reader_segments"]
                )
        if info["lucene_query"] and info["result_sample_count"] != "":
            break
    return info


def load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf"
        if bold
        else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Helvetica Bold.ttf"
        if bold
        else "/System/Library/Fonts/Supplemental/Helvetica.ttf",
    ]
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size=size)
        except OSError:
            pass
    return ImageFont.load_default()


def draw_chart(path: Path, context: dict) -> None:
    width, height = 1600, 1080
    image = Image.new("RGB", (width, height), "#111217")
    draw = ImageDraw.Draw(image)
    colors = [
        "#73BF69",
        "#F2CC0C",
        "#5794F2",
        "#FF9830",
        "#F2495C",
        "#B877D9",
        "#37872D",
        "#FADE2A",
        "#3274D9",
        "#FF6B00",
        "#C4162A",
        "#8F3BB8",
        "#C8F2C2",
        "#FFF899",
        "#C7D8FF",
        "#FFCB7D",
    ]
    text = "#D8DCE7"
    muted = "#A7ADBC"

    draw.text((36, 28), "MongoT search breakdown", fill=text, font=load_font(34, True))
    draw.text((36, 72), context["subtitle"], fill=muted, font=load_font(20))

    card_width = 245
    for index, (label, value) in enumerate(context["cards"]):
        x = 36 + index * (card_width + 14)
        y = 120
        draw.rounded_rectangle((x, y, x + card_width, y + 92), radius=8, fill="#181B20", outline="#2B303B")
        draw.text((x + 16, y + 14), label, fill=muted, font=load_font(17, True))
        draw.text((x + 16, y + 46), value, fill="#73BF69", font=load_font(28, True))

    def stacked_bar(x: int, y: int, w: int, h: int, parts: list[tuple[str, float]], total: float) -> None:
        current = x
        for index, (_, value) in enumerate(parts):
            bar_width = max(1, int(w * value / total)) if total else 1
            draw.rectangle((current, y, current + bar_width, y + h), fill=colors[index % len(colors)])
            current += bar_width
        draw.rectangle((x, y, x + w, y + h), outline="#454B58")

    draw.text((36, 250), "Full stream median", fill=text, font=load_font(24, True))
    stacked_bar(36, 292, 1480, 44, context["stream_parts"], context["root_median"])
    draw.text((44, 348), context["stream_label"], fill=text, font=load_font(18))
    draw.text((640, 348), context["command_label"], fill=text, font=load_font(18))

    draw.text((36, 410), "Command phase median segments", fill=text, font=load_font(24, True))
    command_parts = context["command_parts"]
    stacked_bar(36, 452, 1480, 44, command_parts, sum(value for _, value in command_parts) or 1)
    for index, (name, value) in enumerate(command_parts[:12]):
        x = 36 + (index % 4) * 380
        y = 514 + (index // 4) * 28
        draw.rectangle((x, y + 6, x + 22, y + 18), fill=colors[index % len(colors)])
        draw.text((x + 32, y), f"{name}: {fmt_time_us(value)}", fill=text, font=load_font(17))

    bar_rows = sorted(
        [(name, value) for name, value in context["segment_medians"].items() if name != "stream lifecycle" and value > 0],
        key=lambda item: item[1],
        reverse=True,
    )
    max_value = max((value for _, value in bar_rows), default=1)
    draw.text((36, 630), "Largest command-phase median spans", fill=text, font=load_font(24, True))
    y = 676
    for index, (name, value) in enumerate(bar_rows[:14]):
        draw.text((36, y - 2), name, fill=text, font=load_font(18))
        x = 350
        bar_width = int(900 * value / max_value)
        draw.rectangle((x, y, x + 900, y + 18), fill="#20242C")
        draw.rectangle((x, y, x + bar_width, y + 18), fill=colors[index % len(colors)])
        draw.text((x + 920, y - 2), fmt_time_us(value), fill=muted, font=load_font(18))
        y += 28

    draw.text((36, 1018), context["footer"], fill=muted, font=load_font(18))
    image.save(path)


def segment_definitions(metrics: dict) -> list[tuple[str, list[float], str, str, str]]:
    values = metrics["values"]
    computed = metrics["computed"]
    return [
        (
            "query context",
            values["query context"],
            "`mongot.search.prepare_query_context`",
            "[`SearchCommand.run`](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129), `OptimizationFlagsDefinition.toQueryOptimizationFlags`, `DynamicFeatureFlagRegistry.evaluateClusterInvariant`",
            "Prepares per-query execution context before BSON parsing. This resolves query optimization flags and the dynamic feature flag for the 10k bucket limit. I/O should be limited to in-memory configuration and feature-flag reads.",
        ),
        (
            "parse BSON",
            values["parse BSON"],
            "`mongot.search.parse_query_bson`",
            "[`SearchCommand.run`](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129), [`SearchQuery.fromBson`](../../src/main/java/com/xgen/mongot/index/query/SearchQuery.java)",
            "Parses the incoming `$search` BSON command body into MongoT query model objects. For this workload the result is a collector query with a text operator on `caption` and facet collectors. This is CPU parsing and validation over BSON; it does not invoke Lucene.",
        ),
        (
            "index lookup",
            values["index lookup"],
            "`mongot.search.lookup_index_catalog`",
            "[`SearchCommand.run`](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129), `SearchCommand.getIndexFromCatalog`",
            "Looks up the named search index in MongoT's initialized in-memory catalog and records whether it was found and how many partitions it has. The queried index was `default`. No Lucene call is made here.",
        ),
        (
            "cursor options",
            values["cursor options"],
            "`mongot.search.parse_cursor_options`",
            "[`SearchCommand.run`](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129), `CursorOptionsDefinition.toQueryCursorOptions`",
            "Converts optional cursor settings from the command into `QueryCursorOptions`. The k6 requests do not carry special cursor options, so the median is below trace precision.",
        ),
        (
            "validate",
            values["validate"],
            "`mongot.search.validate_query_options`",
            "[`SearchCommand.validateQueryAndCursorOptions`](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L301)",
            "Performs command-level validation, including checks that reject unsupported cursor options for vector search. The text facet workload takes the normal fast path.",
        ),
        (
            "response mode",
            values["response mode"],
            "`mongot.search.prepare_response_mode`",
            "[`SearchCommand.run`](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129), `SearchCommand.addMetadataIfExplain`, `SearchCommand.determinePopulateCursor`",
            "Sets up explain and dynamic-feature scopes, decides whether the response should include a cursor document, and adds explain metadata when requested. The k6 requests are not explain requests.",
        ),
        (
            "cursor orchestration",
            computed["cursor orchestration"],
            "`mongot.search.create_and_register_cursor` residual",
            "[`MongotCursorManagerImpl.newCursor`](../../src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L130), [`CursorFactory.createCursor`](../../src/main/java/com/xgen/mongot/cursor/CursorFactory.java#L57), [`IndexCursorManagerImpl.createCursor`](../../src/main/java/com/xgen/mongot/cursor/IndexCursorManagerImpl.java#L83)",
            "Residual time inside cursor creation after subtracting explicit cursor setup and Lucene reader query spans. This covers locks, index availability/type checks, cursor ID allocation context, and manager handoff.",
        ),
        (
            "reader orchestration",
            computed["reader orchestration"],
            "`mongot.lucene.prepare_search_reader_query` residual",
            "[`LuceneSearchIndexReader.query`](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java#L250), [`LuceneSearchIndexReader.operatorQuery`](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java#L629), [`LuceneSearchIndexReader.collectorQuery`](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java#L672)",
            "Residual time in the Lucene reader query path after subtracting named Lucene child work. This includes stored-source checks, shutdown shared-lock acquisition, choosing the query branch, extracting the search operator from the collector query, and method dispatch around Lucene state.",
        ),
        (
            "build query",
            values["build query"],
            "`mongot.lucene.build_query`",
            "[`LuceneSearchIndexReader.query`](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java#L295), [`LuceneSearchQueryFactoryDistributor.createQuery`](../../src/main/java/com/xgen/mongot/index/lucene/query/LuceneSearchQueryFactoryDistributor.java#L229), [`TextQueryFactory`](../../src/main/java/com/xgen/mongot/index/lucene/query/TextQueryFactory.java)",
            "Converts the MongoT text/facet operator tree into Lucene [`Query`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/Query.html) objects. This constructs Lucene query objects and may inspect the `IndexReader` for field/query context, but it does not execute the search. The span includes `mongot.lucene.query`, its class, size, hash, and truncation flag.",
        ),
        (
            "Lucene collect hits",
            values["Lucene collect hits"],
            "`mongot.lucene.collect_initial_top_docs`",
            "[`MeteredLuceneSearchManager.initialSearch`](../../src/main/java/com/xgen/mongot/index/lucene/MeteredLuceneSearchManager.java#L30), [`LuceneOperatorSearchManager.initialSearch`](../../src/main/java/com/xgen/mongot/index/lucene/LuceneOperatorSearchManager.java#L34), [`AbstractLuceneSearchManager.createCollectorManager`](../../src/main/java/com/xgen/mongot/index/lucene/AbstractLuceneSearchManager.java#L59)",
            "Actual initial Lucene search execution. MongoT creates a `TopScoreDocCollectorManager` or `TopFieldCollectorManager`, then calls Lucene [`IndexSearcher.search(Query, CollectorManager)`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/IndexSearcher.html#search(org.apache.lucene.search.Query,org.apache.lucene.search.CollectorManager)). Lucene walks index segments, scores/matches the query, and returns [`TopDocs`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/TopDocs.html) for the first batch.",
        ),
        (
            "Lucene advance batch",
            values["Lucene advance batch"],
            "`mongot.cursor.advance_batch_producer`",
            "[`MongotCursor.getExplainDisabledNextBatch`](../../src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L90), [`LuceneSearchBatchProducer.execute`](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L167), [`AbstractLuceneSearchManager.getMoreTopDocs`](../../src/main/java/com/xgen/mongot/index/lucene/AbstractLuceneSearchManager.java#L33)",
            "Advances the batch producer. For the first command batch, it mostly wraps the initial `TopDocs` already collected by `Lucene collect hits`. Later getMore work in the same stream can call Lucene [`IndexSearcher.searchAfter`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/search/IndexSearcher.html#searchAfter(org.apache.lucene.search.ScoreDoc,org.apache.lucene.search.Query,int)) to continue after the previous `ScoreDoc`.",
        ),
        (
            "batch orchestration",
            computed["batch orchestration"],
            "`mongot.search.load_first_cursor_batch` residual",
            "[`MongotCursorManagerImpl.getNextBatch`](../../src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java#L226), [`IndexCursorManagerImpl.getNextBatch`](../../src/main/java/com/xgen/mongot/cursor/IndexCursorManagerImpl.java#L150), [`MongotCursor.getNextBatch`](../../src/main/java/com/xgen/mongot/cursor/MongotCursor.java#L67)",
            "Residual first-batch loading work after subtracting explicit advance and materialization spans. This covers cursor lookups, synchronization, batch-size adjustment, cursor state updates, exhaustion checks, and wrapping result info.",
        ),
        (
            "materialize BSON",
            values["materialize BSON"],
            "`mongot.lucene.materialize_bson_documents`",
            "[`LuceneSearchBatchProducer.getSearchResultsFromIter`](../../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchBatchProducer.java#L299), `SearchResultsIter.acceptAndAdvance`, [`ProjectStage.project`](../../src/main/java/com/xgen/mongot/index/lucene/query/pushdown/project/ProjectStage.java), [`MetaIdRetriever.getRootMetaId`](../../src/main/java/com/xgen/mongot/index/lucene/query/util/MetaIdRetriever.java)",
            "Converts Lucene hits into MongoT BSON result documents. This may read stored fields for stored source projection and metadata through Lucene [`IndexReader.storedFields`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/IndexReader.html#storedFields()) and [`StoredFields`](https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/StoredFields.html). The span records batch count/size and sampled BSON documents.",
        ),
        (
            "response doc",
            values["response doc"],
            "`mongot.search.prepare_response_document`",
            "[`SearchCommand.getBatch`](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317), [`MongotCursorBatch`](../../src/main/java/com/xgen/mongot/cursor/serialization/MongotCursorBatch.java)",
            "Builds the command response wrapper, including optional cursor result and `$$SEARCH_META` variables. It also records the query batch timer sample. No Lucene work happens here.",
        ),
        (
            "command orchestration",
            computed["command orchestration"],
            "`mongot.search.command` residual",
            "[`SearchCommand.run`](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L129), [`SearchCommand.getBatch`](../../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java#L317), [`CursorGuard`](../../src/main/java/com/xgen/mongot/server/command/search/CursorGuard.java)",
            "Residual time inside `mongot.search.command` after subtracting named command-phase spans. This includes root span setup and attributes, metrics increments, explain/feature flag scope management, cursor guard logic, branch handling, and normal return/error control flow.",
        ),
        (
            "stream lifecycle",
            computed["stream lifecycle"],
            "`mongodb.CommandService/atlasSearchCoco.image/search` minus `mongot.search.command`",
            "[`ServerCallHandler.onNext`](../../src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L72), [`ServerCallHandler.handleMessage`](../../src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java#L108), [`CommandManager`](../../src/main/java/com/xgen/mongot/server/grpc/CommandManager.java), [`CommandRegistry.streamLatencyTimer`](../../src/main/java/com/xgen/mongot/server/command/registry/CommandRegistry.java#L170)",
            "Time in the gRPC command stream outside the initial `mongot.search.command`. This includes command dispatch, response observer synchronization, half-close/cancellation lifecycle, client/driver stream consumption, and follow-up cursor getMore work in the same search stream.",
        ),
    ]


def write_markdown(path: Path, context: dict) -> None:
    rows = []
    root_median = context["root_median"]
    command_median = context["command_median"]
    for name, vals, spans, code, desc in context["segment_defs"]:
        value = median(vals)
        if name == "stream lifecycle":
            timing = f"{fmt_time_us(value)}; {pct(value, root_median)} overall; n/a ex-stream"
        else:
            timing = f"{fmt_time_us(value)}; {pct(value, root_median)} overall; {pct(value, command_median)} ex-stream"
        rows.append(f"| {name} | {spans} | {timing} | {code} | {desc} |")

    run_summary = "\n".join(
        [
            "| Metric | Value |",
            "| --- | ---: |",
            f"| k6 requests | {context['k6_requests']:,} |",
            f"| k6 throughput | {context['throughput']} |",
            f"| k6 failures | {context['failed_line']} |",
            f"| k6 HTTP median | {context['http_median']} ms |",
            f"| k6 HTTP p95 | {context['http_p95']} ms |",
            f"| app-reported MongoDB median | {context['mongo_median']} ms |",
            f"| app-reported MongoDB p95 | {context['mongo_p95']} ms |",
            f"| app Java non-Mongo median | {context['java_median']} ms |",
            f"| Jaeger traces sampled | {context['trace_count']:,} |",
            f"| Root trace operation | `{context['operation']}` |",
            f"| Trace query type | `{context['sample']['query_type']}` |",
            f"| Search index | `{context['sample']['index_name']}` |",
            f"| Lucene reader docs / segments | {context['sample']['reader_docs']} docs / {context['sample']['reader_segments']} segments |",
            f"| Median stream span | {fmt_time_us(root_median)} |",
            f"| Median `mongot.search.command` span | {fmt_time_us(command_median)} |",
        ]
    )

    content = f"""# MongoT search trace breakdown

Run window: {context['start_iso']} to {context['end_iso']}.

Command:

```sh
k6 run -e K6_VUS={context['vus']} -e K6_DURATION={context['duration']} k6.js
```

Run summary:

{run_summary}

![Generated search breakdown](./{context['image_name']})

## Query And Index Shape

The k6 script calls `/image/search` without a `searchType`, so the Java app defaults to text search. The app builds a MongoDB aggregation with `$search`, a facet collector, a `compound.must` text clause on `caption`, optional `equals` filter clauses for `hasPerson` and COCO category fields, then `$skip`, `$limit`, and a final `$facet` to return both docs and `$$SEARCH_META`.

Relevant client code:

- [`k6.js`](../../../atlas-search-coco/k6.js)
- [`SearchRequest`](../../../atlas-search-coco/src/main/java/com/mycodefu/mongodb/search/SearchRequest.java)
- [`SearchPipelines`](../../../atlas-search-coco/src/main/java/com/mycodefu/mongodb/search/SearchPipelines.java)
- [`ImageDataAccess`](../../../atlas-search-coco/src/main/java/com/mycodefu/mongodb/ImageDataAccess.java)
- [`atlas-search-index.json`](../../../atlas-search-coco/src/main/resources/atlas-search-index.json)

The text index shape is:

- `caption`: `string`, used by the text operator.
- `hasPerson`: `boolean`, used by optional filter clauses.
- `accessory`, `animal`, `appliance`, `electronic`, `food`, `furniture`, `indoor`, `kitchen`, `outdoor`, `sports`, `vehicle`: each indexed as `token` and `stringFacet`, used by optional equality filters and facet buckets.
- Stored source includes `_id`, caption/image metadata, `hasPerson`, and the category arrays. Because k6 uses `includeLicense=false`, the app requests `returnStoredSource=true`.

The vector index is present in the app, but it was not used by this run because k6 did not send `searchType=Vector` or `searchType=Combined`.

## Timing Method

Segment timings are medians computed from {context['trace_count']:,} Jaeger traces returned for the k6 window. Percentages are computed against the median full gRPC search stream span ({fmt_time_us(root_median)}) and the median initial `mongot.search.command` span ({fmt_time_us(command_median)}).

Later cursor batches are visible inside the stream lifecycle: sampled traces showed later `mongot.lucene.collect_more_top_docs` with median {fmt_time_us(context['later_collect_more'])}, later materialization with median {fmt_time_us(context['later_materialize'])}, and later batch advancement with median {fmt_time_us(context['later_advance'])}.

## Payload Attributes Verified

Representative trace `{context['sample']['trace_id']}` had operation id `{context['sample']['operation_id']}`. The root/server operation name was `{context['operation']}`, matching the parsed namespace and operation rather than the generic command-stream name.

The full input BSON remains on `mongot.search.command` as `mongot.query.document`. The Lucene query span includes:

- `mongot.lucene.query.class`: `{context['sample']['lucene_query_class']}`
- `mongot.lucene.query`: `{context['sample']['lucene_query']}`

The materialization span includes `mongot.result.sample.count={context['sample']['result_sample_count']}` plus sampled `mongot.result.sample.N` BSON payloads, with hash, size, and truncation metadata for large values.

## Trace Segments

| Segment | Span(s) | Timing | Code | Description |
| --- | --- | --- | --- | --- |
{chr(10).join(rows)}

## Takeaways

The in-command MongoT work is sub-millisecond in the sampled traces: median `mongot.search.command` was {fmt_time_us(command_median)}. The representative end-to-end MongoT stream span was {fmt_time_us(root_median)}, dominated by the stream lifecycle outside the initial command. That lifecycle includes gRPC stream handling, client/driver consumption, and later cursor batches.

For the initial command, the largest median contributors in this run were `Lucene collect hits`, `reader orchestration`, `parse BSON`, and `materialize BSON`. The row that directly executes the initial Lucene index query is `Lucene collect hits`. Later cursor batches can perform Lucene `searchAfter` work, captured by the later `mongot.lucene.collect_more_top_docs` median above.
"""
    path.write_text(content)


def build_context(args: argparse.Namespace, run_id: str, start_epoch: int, end_epoch: int, k6_log: Path, traces: list[dict]) -> dict:
    k6_text = k6_log.read_text()
    requests_line = line_value(k6_text, "search_requests")
    requests_parts = requests_line.split()
    k6_requests = int(requests_parts[0]) if requests_parts else 0
    throughput = requests_parts[1].replace("/s", " req/s") if len(requests_parts) > 1 else ""
    http_line = line_value(k6_text, "search_http_time_ms")
    mongo_line = line_value(k6_text, "search_mongodb_time_ms")
    java_line = line_value(k6_text, "search_java_non_mongo_time_ms")

    metrics = calculate_metrics(traces, args.operation)
    values = metrics["values"]
    computed = metrics["computed"]
    segment_defs = segment_definitions(metrics)
    segment_medians = {name: median(vals) for name, vals, *_ in segment_defs}
    root_median = median(values["root"])
    command_median = median(values["command"])
    stream_median = median(computed["stream lifecycle"])
    command_parts = [
        (name, segment_medians[name])
        for name, *_ in segment_defs
        if name != "stream lifecycle" and segment_medians[name] > 0
    ]
    later_collect_more = median(metrics["later"]["Lucene collect more"])
    later_materialize = median(metrics["later"]["materialize BSON"])
    later_advance = median(metrics["later"]["Lucene advance batch"])

    return {
        "run_id": run_id,
        "operation": args.operation,
        "vus": args.vus,
        "duration": args.duration,
        "trace_count": len(traces),
        "start_iso": format_epoch(start_epoch),
        "end_iso": format_epoch(end_epoch),
        "subtitle": f"{format_epoch(start_epoch)} to {format_epoch(end_epoch)} | {args.vus} VUs for {args.duration} | {len(traces):,} Jaeger traces",
        "k6_requests": k6_requests,
        "throughput": throughput,
        "failed_line": line_value(k6_text, "http_req_failed"),
        "http_median": metric_number(http_line, "med"),
        "http_p95": metric_number(http_line, "p(95)"),
        "mongo_median": metric_number(mongo_line, "med"),
        "mongo_p95": metric_number(mongo_line, "p(95)"),
        "java_median": metric_number(java_line, "med"),
        "root_median": root_median,
        "command_median": command_median,
        "cards": [
            ("k6 throughput", throughput),
            ("k6 requests", f"{k6_requests:,}"),
            ("HTTP median", f"{metric_number(http_line, 'med')} ms"),
            ("app MongoDB median", f"{metric_number(mongo_line, 'med')} ms"),
            ("MongoT stream median", fmt_time_us(root_median)),
            ("MongoT command median", fmt_time_us(command_median)),
        ],
        "stream_parts": [("stream lifecycle", stream_median), ("command", command_median)],
        "stream_label": f"stream lifecycle {fmt_time_us(stream_median)} ({pct(stream_median, root_median)})",
        "command_label": f"command {fmt_time_us(command_median)} ({pct(command_median, root_median)})",
        "command_parts": command_parts,
        "segment_medians": segment_medians,
        "segment_defs": segment_defs,
        "later_collect_more": later_collect_more,
        "later_materialize": later_materialize,
        "later_advance": later_advance,
        "footer": f"Later getMore work inside stream lifecycle: Lucene collect more median {fmt_time_us(later_collect_more)}, materialize BSON median {fmt_time_us(later_materialize)}, advance batch median {fmt_time_us(later_advance)}.",
        "sample": sample_payload_info(traces),
        "image_name": f"search-breakdown-{run_id}.png",
    }


def main() -> int:
    args = parse_args()
    args.repo_root = args.repo_root.resolve()
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_k6:
        run_id, start_epoch, end_epoch, k6_log, _ = load_existing_run(args)
    else:
        run_id, start_epoch, end_epoch, k6_log, _ = run_k6(args)

    traces = fetch_jaeger_traces(args, start_epoch, end_epoch)
    context = build_context(args, run_id, start_epoch, end_epoch, k6_log, traces)
    image_path = args.image_path.resolve() if args.image_path else args.output_dir / context["image_name"]
    context["image_name"] = image_path.name
    markdown_path = args.output_dir / f"search-breakdown-{datetime.fromtimestamp(start_epoch).date()}.md"

    draw_chart(image_path, context)
    write_markdown(markdown_path, context)

    print(f"markdown={markdown_path}")
    print(f"image={image_path}")
    print(f"k6_log={k6_log}")
    print(f"traces={len(traces)}")
    print(f"stream_median={fmt_time_us(context['root_median'])}")
    print(f"command_median={fmt_time_us(context['command_median'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
