from typing import NamedTuple


class UploadPartInfo(NamedTuple):
    part_number: int
    start_byte: int
    end_byte: int
    url: str


class PartUploadResult(NamedTuple):
    part_number: int
    bytes_transferred: int
    time_taken: float
    etag: str


class SummaryStats(NamedTuple):
    total_bytes: int
    total_time: float
    average_speed: float
    average_speed_formatted: str
