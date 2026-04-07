#!/usr/bin/env python3
"""
run_pipeline.py – Full pipeline orchestrator.
Runs all 4 steps in sequence for a list of URLs.

Usage:
    python run_pipeline.py --urls urls.txt
    python run_pipeline.py --urls urls.txt --depth 2 --mode both --api-key sk-ant-...
    python run_pipeline.py --urls urls.txt --skip-crawl   # resume from existing raw/
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path


def run_step(label: str, cmd: list[str]) -> bool:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, cwd=Path(__file__).parent)
    if result.returncode != 0:
        print(f"\n❌ Step failed: {label}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Doc Ingestion Pipeline – Full Run")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--urls", help="File with URLs (one per line)")
    group.add_argument("--url", help="Single URL to process")
    parser.add_argument("--output", default="../output", help="Base output directory")
    parser.add_argument("--depth", type=int, default=3, help="Crawl depth")
    parser.add_argument("--delay", type=float, default=0.5, help="Crawl delay between requests")
    parser.add_argument("--max-tokens", type=int, default=800, help="Max tokens per chunk")
    parser.add_argument("--mode", choices=["extract", "qa", "both"], default="extract",
                        help="JSONL generation mode")
    parser.add_argument("--api-key", default="", help="Anthropic API key (for QA mode)")
    parser.add_argument("--skip-crawl", action="store_true", help="Skip crawl step (use existing raw/)")
    parser.add_argument("--skip-index", action="store_true", help="Skip SQLite index step")

    args = parser.parse_args()

    out_dir = Path(args.output)
    raw_dir = out_dir / "raw"
    chunks_dir = out_dir / "chunks"
    jsonl_dir = out_dir / "jsonl"
    db_path = out_dir / "docs.sqlite"

    python = sys.executable
    scripts_dir = Path(__file__).parent

    start = time.time()
    print(f"\n🚀 Doc Ingestion Pipeline Starting")
    print(f"   Output: {out_dir.resolve()}")

    # ── Step 1: Crawl ────────────────────────────────────────────────────────
    if not args.skip_crawl:
        cmd = [python, str(scripts_dir / "01_crawl.py")]
        if args.url:
            cmd += ["--url", args.url]
        else:
            cmd += ["--urls", args.urls]
        cmd += [
            "--output", str(raw_dir),
            "--depth", str(args.depth),
            "--delay", str(args.delay),
        ]
        if not run_step("Step 1/4: Crawling documentation sites", cmd):
            sys.exit(1)
    else:
        print("\n⏭  Skipping crawl (--skip-crawl)")

    # ── Step 2: Chunk ────────────────────────────────────────────────────────
    cmd = [
        python, str(scripts_dir / "02_chunk.py"),
        "--input", str(raw_dir),
        "--output", str(chunks_dir),
        "--max-tokens", str(args.max_tokens),
    ]
    if not run_step("Step 2/4: Chunking Markdown by heading", cmd):
        sys.exit(1)

    # ── Step 3: SQLite Index ─────────────────────────────────────────────────
    if not args.skip_index:
        cmd = [
            python, str(scripts_dir / "03_build_index.py"),
            "--chunks", str(chunks_dir),
            "--output", str(db_path),
        ]
        if not run_step("Step 3/4: Building SQLite FTS index", cmd):
            sys.exit(1)
    else:
        print("\n⏭  Skipping SQLite index (--skip-index)")

    # ── Step 4: JSONL ────────────────────────────────────────────────────────
    cmd = [
        python, str(scripts_dir / "04_generate_jsonl.py"),
        "--chunks", str(chunks_dir),
        "--output", str(jsonl_dir),
        "--mode", args.mode,
    ]
    if args.api_key:
        cmd += ["--api-key", args.api_key]
    if not run_step("Step 4/4: Generating JSONL training data", cmd):
        sys.exit(1)

    # ── Summary ──────────────────────────────────────────────────────────────
    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"  ✅ PIPELINE COMPLETE  ({elapsed:.1f}s)")
    print(f"{'='*60}")
    print(f"\n  Output directory: {out_dir.resolve()}")
    print(f"  ├── raw/           ← cleaned Markdown per page")
    print(f"  ├── chunks/        ← individual chunk .md files")
    print(f"  │   └── chunks_index.json")
    print(f"  ├── jsonl/         ← training data")
    print(f"  │   ├── training_data.jsonl   (all pairs)")
    print(f"  │   ├── raw_retrieval.jsonl")
    print(f"  │   ├── definition.jsonl")
    print(f"  │   ├── procedure.jsonl")
    print(f"  │   └── synthetic_qa.jsonl    (if --mode qa/both)")
    print(f"  └── docs.sqlite    ← FTS5 full-text search index")

    # Read and print jsonl summary if available
    summary_path = jsonl_dir / "summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text())
        print(f"\n  JSONL pairs: {summary['total_pairs']}")
        for t, n in summary.get("by_type", {}).items():
            print(f"    {t:<25} {n:>5}")

    # Quick DB check
    if db_path.exists():
        import sqlite3
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        conn.close()
        print(f"\n  SQLite chunks indexed: {count}")

    print()


if __name__ == "__main__":
    main()
