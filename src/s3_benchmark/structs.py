from typing import NamedTuple


class UploadPartInfo(NamedTuple):
    part_number: int
    start_byte: int
    end_byte: int
    url: str


class DownloadPartInfo(NamedTuple):
    url: str
    range_header: str


class PartDownloadResult(NamedTuple):
    part_number: int
    bytes_transferred: int
    time_taken: float
    error: str | None = None


class PartUploadResult(NamedTuple):
    part_number: int
    bytes_transferred: int
    time_taken: float
    etag: str


class SummaryStats(NamedTuple):
    total_bytes: int
    total_time: float
    std_deviation: float
    average_part_speed: float
    average_speed: float


class RunResult(NamedTuple):
    part_size: str
    part_size_bytes: int
    parallel_parts: int
    total_parts: int
    total_bytes: int
    total_time: float
    average_part_speed: str
    average_speed: str
    std_deviation: str
