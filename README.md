# S3 Presigned Multipart Download Benchmark

Vibe coded benchmark utility.

```
usage: s3-benchmark [-h] [--part-size PART_SIZE [PART_SIZE ...]] [--parallel-parts PARALLEL_PARTS [PARALLEL_PARTS ...]] [--hostname HOSTNAME] [--protocol {http,https}]
                    [--region REGION] [--use-path-style] [--debug] [--verify] [--session-token SESSION_TOKEN]
                    s3_uri

Benchmark S3 object downloads using pre-signed URLs and multi-part downloads.

positional arguments:
  s3_uri                S3 URI of the object to download (s3://bucket-name/object-key)

options:
  -h, --help            show this help message and exit
  --part-size PART_SIZE [PART_SIZE ...]
                        Part sizes to benchmark (e.g., '5MB 10MB 20MB'). Accepts suffixes KB, MB, GB.
  --parallel-parts PARALLEL_PARTS [PARALLEL_PARTS ...]
                        Number of parts to download in parallel for benchmarking (e.g., '5 10 20'). Default: 5
  --hostname HOSTNAME   Custom S3 server hostname (default: AWS S3)
  --protocol {http,https}
                        Protocol to use with custom hostname (default: https)
  --region REGION       AWS region or custom region for S3-compatible server (default: us-east-1)
  --use-path-style      Use path-style addressing instead of virtual-hosted style
  --debug               Enable debug output
  --verify              Store file contents in memory and report MD5 checksum
  --session-token SESSION_TOKEN
                        AWS session token for authentication
```