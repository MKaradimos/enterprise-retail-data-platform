#!/usr/bin/env python3
"""
RetailNova Enterprise Data Platform
======================================
Simple CLI to run the pipeline locally.

Usage:
    python run_pipeline.py --layer all
    python run_pipeline.py --layer bronze
    python run_pipeline.py --layer silver
    python run_pipeline.py --layer gold
    python run_pipeline.py --layer quality
    python run_pipeline.py --layer all --tables customers products
    python run_pipeline.py --layer all --fail-fast
    python run_pipeline.py --tests

Run from the project root directory.
"""

import sys
import os
import argparse
from pathlib import Path

# Add project root to path so imports work
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# Load .env file if it exists
try:
    from dotenv import load_dotenv
    env_file = ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file)
        print(f"[config] Loaded .env from {env_file}")
except ImportError:
    pass  # dotenv optional


def run_tests():
    """Run the full test suite."""
    import subprocess
    result = subprocess.run(
        [sys.executable, "-m", "pytest",
         "tests/test_pipeline.py", "-v", "--tb=short"],
        cwd=str(ROOT)
    )
    sys.exit(result.returncode)


def run_pipeline(args):
    """Import and run the master pipeline."""
    from pipelines.master_pipeline import run_master_pipeline

    run_master_pipeline(
        run_bronze  = args.layer in ("all", "bronze"),
        run_silver  = args.layer in ("all", "silver"),
        run_gold    = args.layer in ("all", "gold"),
        run_quality = not args.skip_quality,
        tables      = args.tables,
        fail_fast   = args.fail_fast,
    )


def print_status():
    """Print current service status."""
    import subprocess
    print("\nService Status:")
    print("─" * 40)
    services = [
        ("SQL Server", "localhost", 1433),
        ("MinIO",      "localhost", 9000),
        ("PostgreSQL", "localhost", 5432),
        ("Spark",      "localhost", 7077),
        ("Jupyter",    "localhost", 8888),
        ("Grafana",    "localhost", 3000),
    ]
    import socket
    for name, host, port in services:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            print(f"  ✓ {name:15} {host}:{port}")
        except:
            print(f"  ✗ {name:15} {host}:{port}  [NOT REACHABLE]")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="RetailNova Enterprise Data Platform Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --layer all
  python run_pipeline.py --layer bronze --tables customers orders
  python run_pipeline.py --layer all --fail-fast --skip-quality
  python run_pipeline.py --tests
  python run_pipeline.py --status
        """
    )
    parser.add_argument(
        "--layer",
        choices=["all", "bronze", "silver", "gold", "quality"],
        default="all",
        help="Which layer(s) to process (default: all)"
    )
    parser.add_argument(
        "--tables", nargs="+", default=None,
        help="Specific source tables for Bronze (e.g. customers products)"
    )
    parser.add_argument(
        "--fail-fast", action="store_true",
        help="Stop pipeline on first failure"
    )
    parser.add_argument(
        "--skip-quality", action="store_true",
        help="Skip data quality checks"
    )
    parser.add_argument(
        "--tests", action="store_true",
        help="Run unit test suite instead of pipeline"
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show service connectivity status"
    )

    args = parser.parse_args()

    if args.status:
        print_status()
        return

    if args.tests:
        run_tests()
        return

    print("\n" + "="*65)
    print("  RETAILNOVA ENTERPRISE DATA PLATFORM")
    print(f"  Mode   : layer={args.layer}")
    print(f"  Tables : {args.tables or 'all'}")
    print("="*65)

    run_pipeline(args)


if __name__ == "__main__":
    main()
