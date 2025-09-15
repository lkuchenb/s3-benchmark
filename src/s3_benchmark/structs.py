from typing import NamedTuple


class UploadPartInfo(NamedTuple):
    part_number: int
    start_byte: int
    end_byte: int
    url: str


class PartUploadResult(NamedTuple):
    part_number: int
    bytes_uploaded: int
    time_taken: float
    etag: str
