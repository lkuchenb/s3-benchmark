import asyncio
import time
import httpx

from s3_benchmark.structs import DownloadPartInfo, PartDownloadResult
from s3_benchmark.utils import SpeedMonitor


class AsyncDownloader:
    """Handle parallel downloads using asyncio and httpx."""

    def __init__(
        self,
        max_concurrent: int,
        speed_monitor: SpeedMonitor,
        stream_transfer: bool = True,
    ):
        """
        Initialize with a speed monitor.

        Args:
            max_concurrent: Maximum number of concurrent downloads guarded by Semaphore
            speed_monitor: SpeedMonitor instance
        """
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.speed_monitor = speed_monitor
        self.stream_transfer = stream_transfer

    async def download_part(
        self, client: httpx.AsyncClient, url_info: DownloadPartInfo, part_number: int
    ) -> PartDownloadResult:
        """
        Download a single part with semaphore for concurrency control.

        Args:
            client: httpx.AsyncClient instance
            url_info: Tuple of (url, range_header)
            part_number: Part number for tracking

        Returns:
            Dict with download metrics
        """
        url, range_header = url_info.url, url_info.range_header

        async with self.semaphore:
            start_time = time.time()
            total_bytes = 0

            try:
                # Explicitly set the Range header in the request
                headers = {"Range": range_header}

                if self.stream_transfer:
                    async with client.stream("GET", url, headers=headers) as response:
                        response.raise_for_status()

                        async for chunk in response.aiter_bytes(chunk_size=65536):
                            total_bytes += len(chunk)

                            # Update the speed monitor
                            await self.speed_monitor.update(len(chunk))
                else:
                    response = await client.get(url, headers=headers)
                    response.raise_for_status()
                    total_bytes = len(response.content)

                    # Update the speed monitor with the total bytes downloaded
                    await self.speed_monitor.update(total_bytes)

            except httpx.HTTPError as exc:
                error_msg = f"\nError downloading part {part_number}: {exc}"
                if hasattr(exc, "response") and exc.response is not None:
                    error_msg += f"\nStatus code: {exc.response.status_code}"
                    error_msg += f"\nResponse body: {exc.response.text}"

                print(error_msg)
                return PartDownloadResult(
                    part_number=part_number,
                    bytes_transferred=total_bytes,
                    time_taken=time.time() - start_time,
                    error=str(exc),
                )

            end_time = time.time()
            # Notify the speed monitor that a part has been completed
            await self.speed_monitor.part_completed()

            return PartDownloadResult(
                part_number=part_number,
                bytes_transferred=total_bytes,
                time_taken=end_time - start_time,
            )

    async def download_all(self, url_infos: list[DownloadPartInfo]) -> list[PartDownloadResult]:
        """
        Download all parts in parallel with concurrency control.

        Args:
            url_infos: List of tuples (url, range_header)
            max_concurrent: Maximum number of concurrent downloads
            debug: Whether to enable debug output

        Returns:
            List of download results
        """

        # Configure httpx client with appropriate settings
        client_kwargs = {"timeout": httpx.Timeout(None), "follow_redirects": True}

        async with httpx.AsyncClient(**client_kwargs) as client:  # type: ignore
            tasks = []
            for i, url_info in enumerate(url_infos):
                tasks.append(self.download_part(client, url_info, i))

            results = await asyncio.gather(*tasks)

            return results
