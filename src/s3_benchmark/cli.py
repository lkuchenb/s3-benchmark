import sys
from s3_benchmark.constants import MODE_DOWNLOAD, MODE_UPLOAD
from s3_benchmark.main import print_tsv_results, run_download_benchmark, run_upload_benchmark
from s3_benchmark.parsing import parse_arguments, parse_s3_uri
from s3_benchmark.utils import CredentialManager, PresignedUrlGenerator, format_size


async def cli():
    """Main entry point for the benchmark tool."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Collect AWS credentials
    credentials = CredentialManager(args.session_token).collect_credentials()
    session = credentials.get_session()
    
    try:
        # Handle download mode
        if args.mode == MODE_DOWNLOAD:
            # Parse S3 URI
            try:
                bucket_name, object_key = parse_s3_uri(args.s3_uri)
            except ValueError as e:
                print(f"Error: {e}")
                sys.exit(1)
                
            # Get object size
            print(f"Getting object metadata for {args.s3_uri}...")
            
            # Initialize URL generator just to get object size
            url_generator = PresignedUrlGenerator(
                session,
                args.hostname,
                args.protocol,
                args.region,
                args.use_path_style
            )
            
            object_size = url_generator.get_object_size(bucket_name, object_key)
            print(f"Object size: {format_size(object_size)}")
            
            # Run benchmarks for all parameter combinations
            benchmark_results = []
            
            print("\nRunning download benchmarks for all parameter combinations...")
            for part_size_bytes in args.part_sizes_bytes:
                for parallel_parts in args.parallel_parts:
                    result = await run_download_benchmark(
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
            
        # Handle upload mode
        elif args.mode == MODE_UPLOAD:
            bucket_name = args.bucket
            object_key = args.key
            file_size_bytes = args.file_size_bytes
            
            print(f"Preparing to upload {format_size(file_size_bytes)} to s3://{bucket_name}/{object_key}")
            
            # Run benchmarks for all parameter combinations
            benchmark_results = []
            
            print("\nRunning upload benchmarks for all parameter combinations...")
            for part_size_bytes in args.part_sizes_bytes:
                for parallel_parts in args.parallel_parts:
                    result = await run_upload_benchmark(
                        session,
                        bucket_name,
                        object_key,
                        file_size_bytes,
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