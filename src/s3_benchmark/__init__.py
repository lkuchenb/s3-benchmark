"""S3 Presigned URL Benchmark Tool."""

import asyncio
import sys

from s3_benchmark.cli import cli


def main():
    try:
        asyncio.run(cli())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)