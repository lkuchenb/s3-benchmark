import asyncio
import time
import httpx
from s3_benchmark.utils import RandomContentGenerator, format_size


class AsyncUploader:
    """Handle parallel uploads using asyncio and httpx."""
    
    def __init__(self, speed_monitor):
        """
        Initialize with a speed monitor.
        
        Args:
            speed_monitor: SpeedMonitor instance
        """
        self.speed_monitor = speed_monitor
        self.content_generator = RandomContentGenerator()
        
    async def upload_part(self, client: httpx.AsyncClient, part_info: dict,
                         semaphore: asyncio.Semaphore, debug: bool = False) -> dict:
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
        url = part_info['url']
        part_number = part_info['part_number']
        start_byte = part_info['start_byte']
        end_byte = part_info['end_byte']
        
        async with semaphore:
            start_time = time.time()
            
            try:
                if debug and part_number == 1:  # First part
                    print(f"\nAttempting to upload part {part_number} with URL: {url}")
                    print(f"Byte range: {start_byte}-{end_byte}")
                
                # Generate content for this part
                content = self.content_generator.generate_content(start_byte, end_byte)
                content_size = len(content)
                
                if debug and part_number == 1:
                    print(f"Generated {format_size(content_size)} of content")
                
                # Upload the part
                headers = {
                    'Content-Length': str(content_size)
                }
                
                async with client.stream("PUT", url, headers=headers, content=content) as response:
                    if debug and part_number == 1:
                        print(f"Response status: {response.status_code}")
                        print(f"Response headers: {response.headers}")
                    
                    response.raise_for_status()
                    etag = response.headers.get('ETag', '').strip('"')
                    
                    if not etag:
                        raise ValueError(f"No ETag received for part {part_number}")
                    
                    # Update the speed monitor with the uploaded bytes
                    await self.speed_monitor.update(content_size)
                    
            except (httpx.HTTPError, ValueError) as e:
                error_msg = f"\nError uploading part {part_number}: {e}"
                if isinstance(e, httpx.HTTPError) and hasattr(e, 'response') and e.response is not None:
                    error_msg += f"\nStatus code: {e.response.status_code}"
                    error_msg += f"\nResponse body: {e.response.text}"
                
                print(error_msg)
                return {
                    "part_number": part_number,
                    "bytes_uploaded": 0,
                    "time_taken": time.time() - start_time,
                    "error": str(e)
                }
            
            end_time = time.time()
            # Notify the speed monitor that a part has been completed
            await self.speed_monitor.part_completed()
            
            return {
                "part_number": part_number,
                "bytes_uploaded": content_size,
                "time_taken": end_time - start_time,
                "etag": etag
            }
    
    async def upload_all(self, upload_info: dict, max_concurrent: int, debug: bool = False) -> tuple[list[dict], str]:
        """
        Upload all parts in parallel with concurrency control.
        
        Args:
            upload_info: Dictionary with upload_id and parts information
            max_concurrent: Maximum number of concurrent uploads
            debug: Whether to enable debug output
            
        Returns:
            Tuple of (list of upload results, upload_id)
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        upload_id = upload_info['upload_id']
        parts = upload_info['parts']
        
        # Configure httpx client with appropriate settings
        client_kwargs = {
            'timeout': httpx.Timeout(None),
            'follow_redirects': True
        }
        
        async with httpx.AsyncClient(**client_kwargs) as client:
            tasks = []
            for part_info in parts:
                tasks.append(self.upload_part(client, part_info, semaphore, debug))
            
            results = await asyncio.gather(*tasks)
            
            return results, upload_id

    def complete_multipart_upload(self, bucket: str, key: str, upload_id: str, parts: list[dict]) -> dict:
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
            {
                'PartNumber': part['part_number'],
                'ETag': part['etag']
            }
            for part in parts
        ]
        
        # Complete the multipart upload
        response = self.s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                'Parts': multipart_parts
            }
        )
        
        print(f"Completed multipart upload: {response}")
            
        return response