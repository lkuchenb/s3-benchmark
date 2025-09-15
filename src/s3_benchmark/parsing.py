import argparse
import re
from s3_benchmark.constants import DEFAULT_PARALLEL_PARTS, MODE_DOWNLOAD, MODE_UPLOAD
from s3_benchmark.utils import parse_size


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Benchmark S3 operations using pre-signed URLs and multi-part transfers."
    )

    # Mode selection
    parser.add_argument(
        "mode",
        choices=[MODE_DOWNLOAD, MODE_UPLOAD],
        help="Operation mode: download or upload",
    )

    parser.add_argument(
        "repeats", type=int, help="Number of times to repeat the benchmark"
    )

    # Download mode arguments
    download_group = parser.add_argument_group("Download mode arguments")
    download_group.add_argument(
        "--s3-uri",
        help="S3 URI of the object to download (s3://bucket-name/object-key)",
    )

    # Upload mode arguments
    upload_group = parser.add_argument_group("Upload mode arguments")
    upload_group.add_argument("--bucket", help="S3 bucket name for upload")
    upload_group.add_argument("--key", help="S3 object key for upload")
    upload_group.add_argument(
        "--file-size",
        type=str,
        help="Size of the file to upload (e.g., '100MB'). Accepts suffixes KB, MB, GB.",
    )

    # Common transfer arguments
    parser.add_argument(
        "--part-size",
        type=str,
        nargs="+",
        default=["8MB"],
        help="Part sizes to benchmark (e.g., '5MB 10MB 20MB'). Accepts suffixes KB, MB, GB.",
    )

    parser.add_argument(
        "--parallel-parts",
        type=int,
        nargs="+",
        default=[DEFAULT_PARALLEL_PARTS],
        help=f"Number of parts to transfer in parallel for benchmarking (e.g., '5 10 20'). Default: {DEFAULT_PARALLEL_PARTS}",
    )

    parser.add_argument(
        "--hostname", type=str, help="Custom S3 server hostname (default: AWS S3)"
    )

    parser.add_argument(
        "--protocol",
        type=str,
        default="https",
        choices=["http", "https"],
        help="Protocol to use with custom hostname (default: https)",
    )

    parser.add_argument(
        "--region",
        type=str,
        default="us-east-1",
        help="AWS region or custom region for S3-compatible server (default: us-east-1)",
    )

    parser.add_argument(
        "--use-path-style",
        action="store_true",
        help="Use path-style addressing instead of virtual-hosted style",
    )

    parser.add_argument("--debug", action="store_true", help="Enable debug output")

    parser.add_argument(
        "--verify",
        action="store_true",
        help="Store file contents in memory and report MD5 checksum",
    )

    parser.add_argument(
        "--session-token", type=str, help="AWS session token for authentication"
    )

    args = parser.parse_args()

    # Validate mode-specific arguments
    if args.mode == MODE_DOWNLOAD and not args.s3_uri:
        parser.error("download mode requires --s3-uri")
    elif args.mode == MODE_UPLOAD and not (args.bucket and args.key and args.file_size):
        parser.error("upload mode requires --bucket, --key, and --file-size")

    # Convert part sizes to bytes
    args.part_sizes_bytes = [parse_size(size) for size in args.part_size]

    # Convert file size to bytes for upload mode
    if args.mode == MODE_UPLOAD:
        args.file_size_bytes = parse_size(args.file_size)

    return args


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """
    Parse S3 URI (s3://bucket-name/object-key) into components.

    Args:
        uri: S3 URI string

    Returns:
        Tuple of (bucket_name, object_key)

    Raises:
        ValueError: If the URI format is invalid
    """
    match = re.match(r"^s3://([^/]+)/(.+)$", uri)
    if not match:
        raise ValueError(
            f"Invalid S3 URI format: {uri}. Expected format: s3://bucket-name/object-key"
        )

    bucket_name, object_key = match.groups()
    return bucket_name, object_key
