import asyncio
from functools import lru_cache
import re
import time
from collections.abc import Generator
import boto3
from botocore.config import Config
from s3_benchmark.constants import MODE_DOWNLOAD
from s3_benchmark.structs import DownloadPartInfo, PartUploadResult, SummaryStats, UploadPartInfo


class CredentialManager:
    """Handle AWS credentials collection and management."""

    def __init__(self):
        self.access_key = None
        self.secret_key = None

    def collect_credentials(self):
        """
        Prompt user for AWS credentials.

        Returns:
            self for method chaining
        """
        print("Enter AWS credentials:")
        self.access_key = input("AWS Access Key ID: ").strip()
        self.secret_key = input("AWS Secret Access Key: ").strip()

        return self

    def get_boto_session(self):
        """
        Create and return a boto3 session with the collected credentials.

        Returns:
            boto3.Session: Configured boto3 session
        """
        session_kwargs = {
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
        }

        return boto3.Session(**session_kwargs)


def get_s3_client(
    boto_session,
    hostname,
    protocol="https",
    region="us-east-1",
    use_path_style=False,
) -> boto3.client:
    """
    Create and return a boto3 S3 client with optional custom configuration.

    Args:
        session: boto3.Session object
        hostname: Optional custom S3 server hostname
        protocol: Protocol to use (http or https)
        region: AWS region or custom region for S3-compatible server
        use_path_style: Whether to use path-style addressing
        debug: Whether to enable debug output

    Returns:
        boto3 S3 client
    """
    # Use custom endpoint
    endpoint_url = f"{protocol}://{hostname}"
    print(f"Using custom endpoint: {endpoint_url}")
    print(f"Region: {region}")
    print(f"Path-style addressing: {use_path_style}")

    s3_client = boto_session.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
        config=Config(s3={"addressing_style": "path" if use_path_style else "auto"}),
    )

    return s3_client


@lru_cache
def generate_content(size) -> bytes:
    """
    Generate deterministic pseudo-random content for a specific byte range.

    Args:
        start_byte: Start byte position
        end_byte: End byte position (inclusive)

    Returns:
        Bytes object containing the generated content
    """
    return b"1" * size


class SpeedMonitor:
    """Track and display transfer speeds and part completion."""

    def __init__(
        self,
        update_interval: float = 0.5,
        total_parts: int = 0,
        speed_window_size: int = 5,
    ):
        """
        Initialize the speed monitor.

        Args:
            update_interval: Interval in seconds for updating the display
            total_parts: Total number of parts to download
            speed_window_size: Number of recent measurements to use for speed calculation
        """
        self.start_time = None
        self.total_bytes = 0
        self.bytes_since_last_update = 0
        self.current_speed = 0
        self.recent_speeds = []
        self.speed_window_size = speed_window_size
        self.update_interval = update_interval
        self.last_update = 0
        self.lock = asyncio.Lock()
        self.completed_parts = 0
        self.total_parts = total_parts
        self.last_line_length = 0  # Track the length of the last printed line

    def start(self):
        """Start monitoring."""
        self.start_time = time.time()
        self.last_update = self.start_time

    async def update(self, bytes_downloaded: int):
        """
        Update with new downloaded data.

        Args:
            bytes_downloaded: Number of bytes downloaded
        """
        async with self.lock:
            self.total_bytes += bytes_downloaded
            self.bytes_since_last_update += bytes_downloaded
            current_time = time.time()

            if current_time - self.last_update >= self.update_interval:
                # Calculate speed based on data downloaded since last update
                time_since_last_update = current_time - self.last_update
                if time_since_last_update > 0:
                    recent_speed = self.bytes_since_last_update / time_since_last_update
                    self.recent_speeds.append(recent_speed)

                    # Keep only the most recent measurements
                    if len(self.recent_speeds) > self.speed_window_size:
                        self.recent_speeds = self.recent_speeds[
                            -self.speed_window_size :
                        ]

                    # Calculate current speed as average of recent speeds
                    self.current_speed = sum(self.recent_speeds) / len(
                        self.recent_speeds
                    )

                self.bytes_since_last_update = 0
                self.last_update = current_time
                await self.display_progress()

    async def part_completed(self):
        """Increment the completed parts counter."""
        async with self.lock:
            self.completed_parts += 1
            await self.display_progress()

    async def display_progress(self):
        """Display current progress, speed, completed parts, and total data transferred."""
        progress_str = f"Current speed: {format_speed(self.current_speed)} | Transferred: {format_size(self.total_bytes)}"
        if self.total_parts > 0:
            progress_str += f" | Parts: {self.completed_parts}/{self.total_parts}"

        # Pad with spaces to overwrite any remaining characters from previous line
        if len(progress_str) < self.last_line_length:
            progress_str += " " * (self.last_line_length - len(progress_str))

        # Update the last line length
        self.last_line_length = len(progress_str)

        # Print with carriage return
        print(f"\r{progress_str}", end="")

    def enrich_results(
        self, results: list[PartUploadResult]
    ) -> SummaryStats:
        """
        Enrich results with total bytes and average speed.

        Args:
            results: List of transfer result dictionaries
        """
        total_bytes = sum(r.bytes_transferred for r in results)
        total_time = time.time() - self.start_time

        part_speeds = [r.time_taken / r.bytes_transferred if r.bytes_transferred > 0 else 0 for r in results]

        average_speed = sum(part_speeds) / len(part_speeds)
        variance = sum(pow(speed - average_speed, 2) for speed in part_speeds) / len(part_speeds)
        stddev = variance**0.5

        return SummaryStats(
            total_bytes=total_bytes,
            total_time=total_time,
            average_speed=average_speed,
            std_deviation=stddev,
        )

    def display_final_stats(self, results: SummaryStats, mode: str = MODE_DOWNLOAD):
        """
        Display final transfer statistics.

        Args:
            results: List of transfer result dictionaries
            mode: Operation mode (download or upload)
        """

        operation = "Download" if mode == MODE_DOWNLOAD else "Upload"
        print(f"\n\n{operation} Benchmark Results:")
        print(f"Total data transferred: {format_size(results.total_bytes)}")
        print(f"Total time: {results.total_time:.2f} seconds")
        print(f"Average {operation.lower()} speed: {results.average_speed_formatted}")


class PresignedUrlGenerator:
    """Generate pre-signed URLs for each part (download or upload)."""

    def __init__(self, s3_client: boto3.client):
        """
        Initialize with a boto3 session.

        Args:
            s3_client: boto3.client object
        """
        self.s3_client = s3_client

    def get_object_size(self, bucket: str, key: str) -> int:
        """
        Get the size of an S3 object.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Size of the object in bytes
        """
        response = self.s3_client.head_object(Bucket=bucket, Key=key)
        return response["ContentLength"]

    def generate_download_urls(
        self,
        bucket: str,
        key: str,
        parts: list[tuple[int, int]],
        expiration: int = 3600,
    ) -> list[DownloadPartInfo]:
        """
        Generate pre-signed URLs for downloading parts.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            parts: List of (start_byte, end_byte) tuples
            expiration: URL expiration time in seconds

        Returns:
            List of tuples (url, range_header)
        """
        urls = []
        for start, end in parts:
            range_header = f"bytes={start}-{end}"
            params = {"Bucket": bucket, "Key": key, "Range": range_header}

            url = self.s3_client.generate_presigned_url(
                "get_object", Params=params, ExpiresIn=expiration
            )

            # Return both the URL and the range header
            urls.append(DownloadPartInfo(url=url, range_header=range_header))

        return urls

    def generate_upload_urls(
        self,
        *,
        bucket: str,
        key: str,
        parts: int,
        upload_id: str,
        expires_in: int = 3600,
    ) -> Generator[dict, None, None]:
        """
        Generate pre-signed URLs for uploading parts.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            parts: List of (start_byte, end_byte) tuples
            expiration: URL expiration time in seconds

        Returns:
            Dictionary with upload_id and parts information
        """

        upload_info = []
        for i, (start, end) in enumerate(parts):
            part_number = i + 1  # S3 part numbers are 1-based

            # Generate presigned URL for this part
            params = {
                "Bucket": bucket,
                "Key": key,
                "UploadId": upload_id,
                "PartNumber": part_number,
            }

            url = self.s3_client.generate_presigned_url(
                "upload_part", Params=params, ExpiresIn=expires_in
            )

            # Store the URL along with part information
            upload_info.append(
                UploadPartInfo(
                    url=url, part_number=part_number, start_byte=start, end_byte=end
                )
            )

        # Return the complete upload information
        return upload_info


def calculate_parts(object_size: int, part_size: int) -> list[tuple[int, int]]:
    """
    Calculate part ranges for multipart download.

    Args:
        object_size: Total size of the object in bytes
        part_size: Size of each part in bytes

    Returns:
        List of (start_byte, end_byte) tuples for each part
    """
    parts = []
    for start in range(0, object_size, part_size):
        end = min(start + part_size - 1, object_size - 1)
        parts.append((start, end))

    return parts


def format_size(size: int) -> str:
    """
    Format size in bytes to human-readable format.

    Args:
        size: Size in bytes

    Returns:
        Formatted size string
    """
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0

    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    return f"{size:.2f} {units[unit_index]}"


def format_speed(speed: float) -> str:
    """
    Format speed in bytes/second to human-readable format.

    Args:
        speed: Speed in bytes per second

    Returns:
        Formatted speed string
    """
    units = ["B/s", "KB/s", "MB/s", "GB/s"]
    unit_index = 0

    while speed >= 1024 and unit_index < len(units) - 1:
        speed /= 1024
        unit_index += 1

    return f"{speed:.2f} {units[unit_index]}"


def parse_size(size_str: str) -> int:
    """
    Parse a size string with optional suffix (KB, MB, GB) to bytes.

    Args:
        size_str: Size string (e.g., "5MB", "10KB", "1GB")

    Returns:
        Size in bytes
    """
    match = re.match(r"^(\d+)([KMG]B)?$", size_str, re.IGNORECASE)
    if not match:
        raise ValueError(
            f"Invalid size format: {size_str}. Expected format: NUMBER[KB|MB|GB]"
        )

    value, unit = match.groups()
    value = int(value)

    if unit:
        unit = unit.upper()
        if unit == "KB":
            value *= 1024
        elif unit == "MB":
            value *= 1024**2
        elif unit == "GB":
            value *= 1024**3

    return value
