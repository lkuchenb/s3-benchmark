#!/usr/bin/env python3
"""
S3 Download Benchmark Tool

A tool to benchmark S3 object downloads using pre-signed URLs and multi-part downloads
with asyncio for parallel processing.
"""

import argparse
import asyncio
import hashlib
import re
import sys
import time
from typing import Dict, List, Tuple

import boto3
from botocore.config import Config
import httpx

# Constants
DEFAULT_PART_SIZE = 5 * 1024 * 1024  # 5 MB
DEFAULT_PARALLEL_PARTS = 5


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Benchmark S3 object downloads using pre-signed URLs and multi-part downloads."
    )
    
    parser.add_argument(
        "s3_uri",
        help="S3 URI of the object to download (s3://bucket-name/object-key)"
    )
    
    parser.add_argument(
        "--part-size",
        type=str,
        nargs='+',
        default=["5MB"],
        help="Part sizes to benchmark (e.g., '5MB 10MB 20MB'). Accepts suffixes KB, MB, GB."
    )
    
    parser.add_argument(
        "--parallel-parts",
        type=int,
        nargs='+',
        default=[DEFAULT_PARALLEL_PARTS],
        help=f"Number of parts to download in parallel for benchmarking (e.g., '5 10 20'). Default: {DEFAULT_PARALLEL_PARTS}"
    )
    
    parser.add_argument(
        "--hostname",
        type=str,
        help="Custom S3 server hostname (default: AWS S3)"
    )
    
    parser.add_argument(
        "--protocol",
        type=str,
        default="https",
        choices=["http", "https"],
        help="Protocol to use with custom hostname (default: https)"
    )
    
    parser.add_argument(
        "--region",
        type=str,
        default="us-east-1",
        help="AWS region or custom region for S3-compatible server (default: us-east-1)"
    )
    
    parser.add_argument(
        "--use-path-style",
        action="store_true",
        help="Use path-style addressing instead of virtual-hosted style"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output"
    )
    
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Store file contents in memory and report MD5 checksum"
    )
    
    parser.add_argument(
        "--session-token",
        type=str,
        help="AWS session token for authentication"
    )
    
    args = parser.parse_args()
    
    # Convert part sizes to bytes
    args.part_sizes_bytes = [parse_size(size) for size in args.part_size]
    
    return args


def parse_size(size_str: str) -> int:
    """
    Parse a size string with optional suffix (KB, MB, GB) to bytes.
    
    Args:
        size_str: Size string (e.g., "5MB", "10KB", "1GB")
        
    Returns:
        Size in bytes
    """
    match = re.match(r'^(\d+)([KMG]B)?$', size_str, re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid size format: {size_str}. Expected format: NUMBER[KB|MB|GB]")
    
    value, unit = match.groups()
    value = int(value)
    
    if unit:
        unit = unit.upper()
        if unit == 'KB':
            value *= 1024
        elif unit == 'MB':
            value *= 1024 * 1024
        elif unit == 'GB':
            value *= 1024 * 1024 * 1024
    
    return value


class CredentialManager:
    """Handle AWS credentials collection and management."""
    
    def __init__(self, session_token=None):
        self.access_key = None
        self.secret_key = None
        self.session_token = session_token
    
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
    
    def get_session(self):
        """
        Create and return a boto3 session with the collected credentials.
        
        Returns:
            boto3.Session: Configured boto3 session
        """
        session_kwargs = {
            'aws_access_key_id': self.access_key,
            'aws_secret_access_key': self.secret_key,
        }
        
        if self.session_token:
            session_kwargs['aws_session_token'] = self.session_token
            
        return boto3.Session(**session_kwargs)


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """
    Parse S3 URI (s3://bucket-name/object-key) into components.
    
    Args:
        uri: S3 URI string
        
    Returns:
        Tuple of (bucket_name, object_key)
        
    Raises:
        ValueError: If the URI format is invalid
    """
    match = re.match(r'^s3://([^/]+)/(.+)$', uri)
    if not match:
        raise ValueError(
            f"Invalid S3 URI format: {uri}. Expected format: s3://bucket-name/object-key"
        )
    
    bucket_name, object_key = match.groups()
    return bucket_name, object_key


def calculate_parts(object_size: int, part_size: int) -> List[Tuple[int, int]]:
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


class PresignedUrlGenerator:
    """Generate pre-signed URLs for each part."""
    
    def __init__(self, session, hostname=None, protocol="https", region="us-east-1",
                 use_path_style=False, debug=False):
        """
        Initialize with a boto3 session.
        
        Args:
            session: boto3.Session object
            hostname: Optional custom S3 server hostname
            protocol: Protocol to use (http or https)
            region: AWS region or custom region for S3-compatible server
            use_path_style: Whether to use path-style addressing
            debug: Whether to enable debug output
        """
        self.debug = debug
        
        if hostname:
            # Use custom endpoint
            endpoint_url = f"{protocol}://{hostname}"
            if self.debug:
                print(f"Using custom endpoint: {endpoint_url}")
                print(f"Region: {region}")
                print(f"Path-style addressing: {use_path_style}")
            
            self.s3_client = session.client(
                's3',
                endpoint_url=endpoint_url,
                region_name=region,
                config=Config(
                    s3={'addressing_style': 'path' if use_path_style else 'auto'}
                )
            )
        else:
            if self.debug:
                print(f"Using AWS S3 with region: {region}")
            self.s3_client = session.client('s3', region_name=region)
    
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
        return response['ContentLength']
    
    def generate_urls(self, bucket: str, key: str, parts: List[Tuple[int, int]],
                     expiration: int = 3600) -> List[Tuple[str, str]]:
        """
        Generate pre-signed URLs for all parts.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            parts: List of (start_byte, end_byte) tuples
            expiration: URL expiration time in seconds
            
        Returns:
            List of tuples (url, range_header)
        """
        result = []
        for i, (start, end) in enumerate(parts):
            range_header = f"bytes={start}-{end}"
            params = {
                'Bucket': bucket,
                'Key': key,
                'Range': range_header
            }
            
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params=params,
                ExpiresIn=expiration
            )
            
            if self.debug and i == 0:  # Only print the first URL to avoid flooding the console
                print(f"Sample presigned URL (part 0): {url}")
                print(f"Range header: {range_header}")
            
            # Return both the URL and the range header
            result.append((url, range_header))
        
        return result


class SpeedMonitor:
    """Track and display download speeds and part completion."""
    
    def __init__(self, update_interval: float = 0.5, total_parts: int = 0, speed_window_size: int = 5):
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
                        self.recent_speeds = self.recent_speeds[-self.speed_window_size:]
                    
                    # Calculate current speed as average of recent speeds
                    self.current_speed = sum(self.recent_speeds) / len(self.recent_speeds)
                
                self.bytes_since_last_update = 0
                self.last_update = current_time
                await self.display_progress()
    
    async def part_completed(self):
        """Increment the completed parts counter."""
        async with self.lock:
            self.completed_parts += 1
            await self.display_progress()
    
    async def display_progress(self):
        """Display current progress, speed, completed parts, and total data downloaded."""
        progress_str = f"Current download speed: {format_speed(self.current_speed)} | Downloaded: {format_size(self.total_bytes)}"
        if self.total_parts > 0:
            progress_str += f" | Parts: {self.completed_parts}/{self.total_parts}"
        
        # Pad with spaces to overwrite any remaining characters from previous line
        if len(progress_str) < self.last_line_length:
            progress_str += ' ' * (self.last_line_length - len(progress_str))
        
        # Update the last line length
        self.last_line_length = len(progress_str)
        
        # Print with carriage return
        print(f"\r{progress_str}", end="")
    
    def get_average_speed(self) -> float:
        """
        Get average download speed.
        
        Returns:
            Average speed in bytes per second
        """
        if not self.speeds:
            return 0
        return sum(self.speeds) / len(self.speeds)
    
    def display_final_stats(self, results: List[Dict]):
        """
        Display final download statistics.
        
        Args:
            results: List of download result dictionaries
        """
        total_bytes = sum(r.get("bytes_downloaded", 0) for r in results)
        total_time = time.time() - self.start_time
        
        if total_time > 0:
            average_speed = total_bytes / total_time
        else:
            average_speed = 0
        
        print("\n\nDownload Benchmark Results:")
        print(f"Total data downloaded: {format_size(total_bytes)}")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Average download speed: {format_speed(average_speed)}")
        
        # Check for errors
        errors = [r for r in results if "error" in r]
        if errors:
            print(f"\nWarning: {len(errors)} part(s) had errors during download.")


class AsyncDownloader:
    """Handle parallel downloads using asyncio and httpx."""
    
    def __init__(self, speed_monitor, verify=False):
        """
        Initialize with a speed monitor.
        
        Args:
            speed_monitor: SpeedMonitor instance
            verify: Whether to store file contents and calculate MD5 checksum
        """
        self.speed_monitor = speed_monitor
        self.verify = verify
        self.part_data = {}  # Store downloaded data by part number when verify=True
    
    async def download_part(self, client: httpx.AsyncClient, url_info: Tuple[str, str],
                           part_number: int, semaphore: asyncio.Semaphore, debug: bool = False) -> Dict:
        """
        Download a single part with semaphore for concurrency control.
        
        Args:
            client: httpx.AsyncClient instance
            url_info: Tuple of (url, range_header)
            part_number: Part number for tracking
            semaphore: Asyncio semaphore for concurrency control
            debug: Whether to enable debug output
        
        Returns:
            Dict with download metrics
        """
        url, range_header = url_info
        
        async with semaphore:
            start_time = time.time()
            total_bytes = 0
            
            try:
                if debug and part_number == 0:
                    print(f"\nAttempting to download part {part_number} with URL: {url}")
                    print(f"Using Range header: {range_header}")
                
                # Explicitly set the Range header in the request
                headers = {
                    'Range': range_header
                }
                
                async with client.stream("GET", url, headers=headers) as response:
                    if debug and part_number == 0:
                        print(f"Response status: {response.status_code}")
                        print(f"Response headers: {response.headers}")
                        if 'Content-Range' in response.headers:
                            print(f"Content-Range: {response.headers['Content-Range']}")
                        if 'Content-Length' in response.headers:
                            print(f"Content-Length: {response.headers['Content-Length']}")
                    
                    response.raise_for_status()
                    
                    # Initialize data storage for this part if verification is enabled
                    if self.verify:
                        part_buffer = bytearray()
                    
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        total_bytes += len(chunk)
                        
                        # Store the data if verification is enabled
                        if self.verify:
                            part_buffer.extend(chunk)
                            
                        # Update the speed monitor
                        await self.speed_monitor.update(len(chunk))
                    
                    # Store the complete part data if verification is enabled
                    if self.verify:
                        self.part_data[part_number] = part_buffer
                        
            except httpx.HTTPError as e:
                error_msg = f"\nError downloading part {part_number}: {e}"
                if hasattr(e, 'response') and e.response is not None:
                    error_msg += f"\nStatus code: {e.response.status_code}"
                    error_msg += f"\nResponse body: {e.response.text}"
                
                print(error_msg)
                return {
                    "part_number": part_number,
                    "bytes_downloaded": total_bytes,
                    "time_taken": time.time() - start_time,
                    "error": str(e)
                }
                
            end_time = time.time()
            # Notify the speed monitor that a part has been completed
            await self.speed_monitor.part_completed()
            
            return {
                "part_number": part_number,
                "bytes_downloaded": total_bytes,
                "time_taken": end_time - start_time
            }
    
    async def download_all(self, url_infos: List[Tuple[str, str]], max_concurrent: int, debug: bool = False) -> List[Dict]:
        """
        Download all parts in parallel with concurrency control.
        
        Args:
            url_infos: List of tuples (url, range_header)
            max_concurrent: Maximum number of concurrent downloads
            debug: Whether to enable debug output
        
        Returns:
            List of download results
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        # Configure httpx client with appropriate settings
        client_kwargs = {
            'timeout': httpx.Timeout(None),
            'follow_redirects': True
        }
        
        async with httpx.AsyncClient(**client_kwargs) as client:
            tasks = []
            for i, url_info in enumerate(url_infos):
                tasks.append(self.download_part(client, url_info, i, semaphore, debug))
            
            results = await asyncio.gather(*tasks)
            
            # Calculate MD5 checksum if verification is enabled
            if self.verify:
                self.calculate_checksum()
                
            return results
    
    def calculate_checksum(self):
        """Calculate and display MD5 checksum of the downloaded file."""
        if not self.verify or not self.part_data:
            return
            
        print("\nCalculating MD5 checksum...")
        md5 = hashlib.md5()
        
        # Process parts in order
        for part_num in sorted(self.part_data.keys()):
            md5.update(self.part_data[part_num])
            
        print(f"MD5 Checksum: {md5.hexdigest()}")


def format_size(size: int) -> str:
    """
    Format size in bytes to human-readable format.
    
    Args:
        size: Size in bytes
        
    Returns:
        Formatted size string
    """
    units = ['B', 'KB', 'MB', 'GB', 'TB']
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
    units = ['B/s', 'KB/s', 'MB/s', 'GB/s']
    unit_index = 0
    
    while speed >= 1024 and unit_index < len(units) - 1:
        speed /= 1024
        unit_index += 1
        
    return f"{speed:.2f} {units[unit_index]}"


async def run_benchmark(session, bucket_name, object_key, part_size_bytes, parallel_parts, args):
    """
    Run a single benchmark with specific parameters.
    
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
    # Initialize URL generator
    url_generator = PresignedUrlGenerator(
        session,
        args.hostname,
        args.protocol,
        args.region,
        args.use_path_style,
        args.debug
    )
    
    # Get object size (only needed once, but included here for completeness)
    object_size = url_generator.get_object_size(bucket_name, object_key)
    
    # Calculate part ranges
    parts = calculate_parts(object_size, part_size_bytes)
    print(f"\nBenchmarking with part size {format_size(part_size_bytes)} and {parallel_parts} parallel parts...")
    print(f"Downloading in {len(parts)} parts")
    
    # Generate pre-signed URLs with range headers
    url_infos = url_generator.generate_urls(bucket_name, object_key, parts)
    
    # Initialize speed monitor with total parts count
    speed_monitor = SpeedMonitor(total_parts=len(parts))
    speed_monitor.start()
    
    # Initialize downloader
    downloader = AsyncDownloader(speed_monitor, verify=args.verify)
    
    # Start downloads
    start_time = time.time()
    results = await downloader.download_all(url_infos, parallel_parts, args.debug)
    total_time = time.time() - start_time
    
    # Calculate total bytes and average speed
    total_bytes = sum(r.get("bytes_downloaded", 0) for r in results)
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
        "average_speed_formatted": format_speed(average_speed)
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
        key=lambda x: (x.get("part_size_bytes", 0), x.get("parallel_parts", 0))
    )
    
    # Print TSV header
    print("\nBenchmark Results (TSV format):")
    print("Part Size\tParallel Parts\tTotal Time (s)\tAverage Speed")
    
    # Print TSV rows
    for result in sorted_results:
        print(f"{result['part_size']}\t{result['parallel_parts']}\t{result['total_time']:.2f}\t{result['average_speed_formatted']}")


async def cli():
    """Main entry point for the benchmark tool."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Collect AWS credentials
    credentials = CredentialManager(args.session_token).collect_credentials()
    session = credentials.get_session()
    
    # Parse S3 URI
    try:
        bucket_name, object_key = parse_s3_uri(args.s3_uri)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    try:
        # Get object size (only need to do this once)
        print(f"Getting object metadata for {args.s3_uri}...")
        
        # Initialize URL generator just to get object size
        url_generator = PresignedUrlGenerator(
            session,
            args.hostname,
            args.protocol,
            args.region,
            args.use_path_style,
            args.debug
        )
        
        object_size = url_generator.get_object_size(bucket_name, object_key)
        print(f"Object size: {format_size(object_size)}")
        
        # Run benchmarks for all parameter combinations
        benchmark_results = []
        
        print("\nRunning benchmarks for all parameter combinations...")
        for part_size_bytes in args.part_sizes_bytes:
            for parallel_parts in args.parallel_parts:
                result = await run_benchmark(
                    session,
                    bucket_name,
                    object_key,
                    part_size_bytes,
                    parallel_parts,
                    args
                )
                benchmark_results.append(result)
        
        # Print results as TSV table
        print_tsv_results(benchmark_results)
        
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


def main():
    try:
        asyncio.run(cli())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)