import asyncio
import hashlib
import time
import httpx


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
    
    async def download_part(self, client: httpx.AsyncClient, url_info: tuple[str, str],
                           part_number: int, semaphore: asyncio.Semaphore, debug: bool = False) -> dict:
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

    async def download_all(self, url_infos: list[tuple[str, str]], max_concurrent: int, debug: bool = False) -> list[dict]:
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