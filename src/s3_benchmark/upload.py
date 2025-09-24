import asyncio
import time
import boto3
import httpx
from s3_benchmark.structs import PartUploadResult, UploadPartInfo
from s3_benchmark.utils import generate_content, SpeedMonitor


class AsyncUploader:
    """Handle parallel uploads using asyncio and httpx."""

    def __init__(
        self,
        *,
        max_concurrent: int,
        speed_monitor: SpeedMonitor,
        s3_client: boto3.client,
        stream_transfer: bool = True,
    ):
        """
        Initialize with a speed monitor.

        Args:
            speed_monitor: SpeedMonitor instance
        """
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(None), follow_redirects=True
        )
        self.s3_client = s3_client
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.speed_monitor = speed_monitor
        self.stream_transfer = stream_transfer

    async def create_multipart_upload(self, bucket: str, key: str) -> str:
        """
        Initiate a multipart upload and return the upload ID.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Upload ID
        """
        await self.client.__aenter__()
        response = self.s3_client.create_multipart_upload(Bucket=bucket, Key=key)
        return response["UploadId"]

    async def upload_part(self, part_info: UploadPartInfo) -> PartUploadResult:
        """
        Upload a single part with semaphore for concurrency control.

        Args:
            client: httpx.AsyncClient instance
            part_info: Dictionary with part information
            semaphore: Asyncio semaphore for concurrency control
            debug: Whether to enable debug output

        Returns:
            Dict with upload metrics
        """
        url = part_info.url
        part_number = part_info.part_number
        start_byte = part_info.start_byte
        end_byte = part_info.end_byte

        async with self.semaphore:
            start_time = time.time()

            try:
                # Generate content for this part
                content = generate_content(end_byte - start_byte + 1)
                content_size = len(content)

                # Upload the part
                headers = {"Content-Length": str(content_size)}

                if self.stream_transfer:
                    async with self.client.stream(
                        "PUT", url, headers=headers, content=content
                    ) as response:
                        response.raise_for_status()
                        etag = response.headers.get("ETag", "").strip('"')

                        if not etag:
                            raise ValueError(f"No ETag received for part {part_number}")

                        # Update the speed monitor with the uploaded bytes
                        await self.speed_monitor.update(content_size)
                else:
                    response = await self.client.put(
                        url, headers=headers, content=content
                    )
                    response.raise_for_status()
                    etag = response.headers.get("ETag", "").strip('"')

                    if not etag:
                        raise ValueError(f"No ETag received for part {part_number}")

                    # Update the speed monitor with the uploaded bytes
                    await self.speed_monitor.update(content_size)

            except (httpx.HTTPError, ValueError) as exc:
                raise asyncio.CancelledError(
                    f"\nError uploading part {part_number}: {exc}"
                )

            end_time = time.time()
            # Notify the speed monitor that a part has been completed
            await self.speed_monitor.part_completed()

            return PartUploadResult(
                part_number=part_number,
                bytes_transferred=content_size,
                time_taken=end_time - start_time,
                etag=etag,
            )

    async def upload_all(
        self, upload_info: list[UploadPartInfo]
    ) -> list[PartUploadResult]:
        """
        Upload all parts in parallel with concurrency control.

        Args:
            upload_info: Dictionary with upload_id and parts information
            max_concurrent: Maximum number of concurrent uploads
            debug: Whether to enable debug output

        Returns:
            Tuple of (list of upload results, upload_id)
        """

        tasks = []
        for part_info in upload_info:
            tasks.append(self.upload_part(part_info))
        results = await asyncio.gather(*tasks)

        return results

    async def complete_multipart_upload(
        self, bucket: str, key: str, upload_id: str, parts: list[PartUploadResult]
    ) -> dict:
        """
        Complete a multipart upload.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            upload_id: Multipart upload ID
            parts: List of completed parts with ETag information

        Returns:
            Response from S3 complete_multipart_upload API
        """
        # Format the parts information as required by S3 API
        multipart_parts = [
            {"PartNumber": part.part_number, "ETag": part.etag} for part in parts
        ]

        # Complete the multipart upload
        response = self.s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": multipart_parts},
        )

        print(f"Completed multipart upload: {response}")
        await self.client.__aexit__(None, None, None)

        return response
