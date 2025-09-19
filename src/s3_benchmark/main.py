#!/usr/bin/env python3
"""
S3 Benchmark Tool

A tool to benchmark S3 operations using pre-signed URLs and multi-part transfers
with asyncio for parallel processing. Supports both download and upload modes.
"""

from s3_benchmark.constants import MODE_DOWNLOAD, MODE_UPLOAD
from s3_benchmark.download import AsyncDownloader
from s3_benchmark.upload import AsyncUploader
from s3_benchmark.utils import (
    PresignedUrlGenerator,
    SpeedMonitor,
    calculate_parts,
    format_size,
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
    # Calculate part ranges
    parts = calculate_parts(file_size_bytes, part_size_bytes)
    print(
        f"\nBenchmarking with part size {format_size(part_size_bytes)} and {parallel_parts} parallel parts..."
    )
    print(f"Uploading in {len(parts)} parts")

    s3_client = get_s3_client(
        boto_session, args.hostname, args.protocol, args.region, args.use_path_style
    )
    url_generator = PresignedUrlGenerator(s3_client)

    # Initialize speed monitor with total parts count
    speed_monitor = SpeedMonitor(total_parts=len(parts))
    speed_monitor.start()

    # Initialize uploader
    uploader = AsyncUploader(
        max_concurrent=parallel_parts,
        speed_monitor=speed_monitor,
        s3_client=s3_client,
    )

    upload_id = await uploader.create_multipart_upload(bucket_name, object_key)
    print(f"Initiated multipart upload with Upload ID: {upload_id}")

    # Generate pre-signed URLs for upload
    upload_info = url_generator.generate_upload_urls(
        bucket=bucket_name, key=object_key, parts=parts, upload_id=upload_id
    )
    print(f"Generated {len(upload_info)} pre-signed URLs for upload")

    # Start uploads
    results = await uploader.upload_all(upload_info)
    print(f"Completed uploading {len(results)} parts")

    # Complete the multipart upload
    try:
        await uploader.complete_multipart_upload(
            bucket_name, object_key, upload_id, results
        )
    except Exception as e:
        print(f"\nError completing multipart upload: {e}")

    # Display final stats
    summary_stats = speed_monitor.enrich_results(results, MODE_UPLOAD)
    speed_monitor.display_final_stats(summary_stats, MODE_UPLOAD)

    # Return benchmark results
    return {
        "part_size": format_size(part_size_bytes),
        "part_size_bytes": part_size_bytes,
        "parallel_parts": parallel_parts,
        "total_parts": len(parts),
        "total_bytes": summary_stats.total_bytes,
        "total_time": summary_stats.total_time,
        "average_speed": summary_stats.average_speed,
        "average_speed_formatted": summary_stats.average_speed_formatted,
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
    print(f"Object size: {format_size(object_size)}")

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
    results = await downloader.download_all(url_infos)

    # Display final stats
    summary_stats = speed_monitor.enrich_results(results, MODE_DOWNLOAD)
    speed_monitor.display_final_stats(summary_stats, MODE_DOWNLOAD)

    # Return benchmark results
    return {
        "part_size": format_size(part_size_bytes),
        "part_size_bytes": part_size_bytes,
        "parallel_parts": parallel_parts,
        "total_parts": len(parts),
        "total_bytes": summary_stats.total_bytes,
        "total_time": summary_stats.total_time,
        "average_speed": summary_stats.average_speed,
        "average_speed_formatted": summary_stats.average_speed_formatted,
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
