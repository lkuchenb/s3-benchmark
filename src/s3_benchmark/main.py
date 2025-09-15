#!/usr/bin/env python3
"""
S3 Benchmark Tool

A tool to benchmark S3 operations using pre-signed URLs and multi-part transfers
with asyncio for parallel processing. Supports both download and upload modes.
"""

import time

from s3_benchmark.constants import MODE_DOWNLOAD, MODE_UPLOAD
from s3_benchmark.download import AsyncDownloader
from s3_benchmark.upload import AsyncUploader
from s3_benchmark.utils import (
    PresignedUrlGenerator,
    SpeedMonitor,
    calculate_parts,
    format_size,
    format_speed,
    get_s3_client,
)


async def run_upload_benchmark(
    boto_session,
    bucket_name,
    object_key,
    file_size_bytes,
    part_size_bytes,
    parallel_parts,
    args,
):
    """
    Run a single upload benchmark with specific parameters.

    Args:
        session: boto3 session
        bucket_name: S3 bucket name
        object_key: S3 object key
        file_size_bytes: Size of the file to upload in bytes
        part_size_bytes: Size of each part in bytes
        parallel_parts: Number of parts to upload in parallel
        args: Command line arguments

    Returns:
        Dict with benchmark results
    """

    s3_client = get_s3_client(
        boto_session, args.hostname, args.protocol, args.region, args.use_path_style
    )
    url_generator = PresignedUrlGenerator(s3_client)

    # Calculate part ranges
    parts = calculate_parts(file_size_bytes, part_size_bytes)
    print(
        f"\nBenchmarking with part size {format_size(part_size_bytes)} and {parallel_parts} parallel parts..."
    )
    print(f"Uploading in {len(parts)} parts")

    # Generate pre-signed URLs for upload
    upload_info = url_generator.generate_upload_urls(bucket_name, object_key, parts)

    # Initialize speed monitor with total parts count
    speed_monitor = SpeedMonitor(total_parts=len(parts))
    speed_monitor.start()

    # Initialize uploader
    uploader = AsyncUploader(
        max_concurrent=args.parallel_uploads,
        speed_monitor=speed_monitor,
        s3_client=s3_client,
    )

    # Start uploads
    start_time = time.time()
    results, upload_id = await uploader.upload_all(upload_info)

    # Complete the multipart upload if there were no errors
    if not any("error" in r for r in results):
        try:
            await uploader.complete_multipart_upload(
                bucket_name, object_key, upload_id, [r for r in results if "etag" in r]
            )
        except Exception as e:
            print(f"\nError completing multipart upload: {e}")

    total_time = time.time() - start_time

    # Display final stats
    speed_monitor.display_final_stats(results, MODE_UPLOAD)

    # Calculate total bytes and average speed
    total_bytes = sum(r.get("bytes_uploaded", 0) for r in results)
    if total_time > 0:
        average_speed = total_bytes / total_time
    else:
        average_speed = 0

    # Return benchmark results
    return {
        "part_size": format_size(part_size_bytes),
        "part_size_bytes": part_size_bytes,
        "parallel_parts": parallel_parts,
        "total_parts": len(parts),
        "total_bytes": total_bytes,
        "total_time": total_time,
        "average_speed": average_speed,
        "average_speed_formatted": format_speed(average_speed),
    }


async def run_download_benchmark(
    boto_session, bucket_name, object_key, part_size_bytes, parallel_parts, args
):
    """
    Run a single download benchmark with specific parameters.

    Args:
        session: boto3 session
        bucket_name: S3 bucket name
        object_key: S3 object key
        part_size_bytes: Size of each part in bytes
        parallel_parts: Number of parts to download in parallel
        args: Command line arguments

    Returns:
        Dict with benchmark results
    """
    s3_client = get_s3_client(
        boto_session, args.hostname, args.protocol, args.region, args.use_path_style
    )
    url_generator = PresignedUrlGenerator(s3_client)

    # Get object size
    object_size = url_generator.get_object_size(bucket_name, object_key)

    # Calculate part ranges
    parts = calculate_parts(object_size, part_size_bytes)
    print(
        f"\nBenchmarking with part size {format_size(part_size_bytes)} and {parallel_parts} parallel parts..."
    )
    print(f"Downloading in {len(parts)} parts")

    # Generate pre-signed URLs with range headers
    url_infos = url_generator.generate_download_urls(bucket_name, object_key, parts)

    # Initialize speed monitor with total parts count
    speed_monitor = SpeedMonitor(total_parts=len(parts))
    speed_monitor.start()

    # Initialize downloader
    downloader = AsyncDownloader(
        max_concurrent=parallel_parts, speed_monitor=speed_monitor
    )

    # Start downloads
    start_time = time.time()
    results = await downloader.download_all(url_infos)
    total_time = time.time() - start_time

    # Display final stats
    speed_monitor.display_final_stats(results, MODE_DOWNLOAD)

    # Calculate total bytes and average speed
    total_bytes = sum(r.get("bytes_downloaded", 0) for r in results)
    average_speed = max(0, total_bytes / total_time)

    # Return benchmark results
    return {
        "part_size": format_size(part_size_bytes),
        "part_size_bytes": part_size_bytes,
        "parallel_parts": parallel_parts,
        "total_parts": len(parts),
        "total_bytes": total_bytes,
        "total_time": total_time,
        "average_speed": average_speed,
        "average_speed_formatted": format_speed(average_speed),
    }


def print_tsv_results(benchmark_results):
    """
    Print benchmark results as a TSV table.

    Args:
        benchmark_results: List of benchmark result dictionaries
    """
    # Sort results by part size and parallel parts
    sorted_results = sorted(
        benchmark_results,
        key=lambda x: (x.get("part_size_bytes", 0), x.get("parallel_parts", 0)),
    )

    # Print TSV header
    print("\nBenchmark Results (TSV format):")
    print("Part Size\tParallel Parts\tTotal Time (s)\tAverage Speed")

    # Print TSV rows
    for result in sorted_results:
        print(
            f"{result['part_size']}\t{result['parallel_parts']}\t{result['total_time']:.2f}\t{result['average_speed_formatted']}"
        )
