import subprocess
from abc import ABC, abstractmethod
import csv
import gzip
import os
import requests


class Downloader(ABC):
    @abstractmethod
    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        pass


class CCDownloader(Downloader):
    def __init__(self, crawl_path: str, logger, batcher_monitor) -> None:
        self.crawl_path = crawl_path
        self.logger = logger
        self.batcher_monitor = batcher_monitor

    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        headers = {"Range": f"bytes={start}-{start+length-1}"}
        try:
            response = requests.get(f"{self.crawl_path}/{url}", headers=headers)
            response.raise_for_status()
            buffer = response.content
            return gzip.decompress(buffer)
        except requests.RequestException as e:
            self.logger.info(f"Error for prefix URL: {self.crawl_path}/{url}, Failed to fetch response: {e}")
            self.batcher_monitor.increment_counter(
                "batcher_failed_url_cdx_chunk_download"
            )
            return b""  # Return an empty byte object as a fallback


class IndexReader(ABC):
    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def get_size(self):
        pass


class CSVIndexReader(IndexReader):
    def __init__(self, filename: str) -> None:
        if not os.path.exists(filename):
            raise FileNotFoundError(f"The file '{filename}' does not exist.")
        self.file = open(filename, "r")
        self.reader = csv.reader(self.file, delimiter="\t")

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.reader)

    def __del__(self) -> None:
        self.file.close()

    def get_size(self):
        """Get the total number of rows in the file using wc -l."""
        result = subprocess.run(
            ["wc", "-l", self.file.name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Error counting lines: {result.stderr}")
        line_count = int(result.stdout.split()[0])
        return line_count


def test_can_read_index(tmp_path):
    filename = tmp_path / "test.csv"
    index = "0,100,22,165)/ 20240722120756	cdx-00000.gz	0	188224	1\n\
101,141,199,66)/robots.txt 20240714155331	cdx-00000.gz	188224	178351	2\n\
104,223,1,100)/ 20240714230020	cdx-00000.gz	366575	178055	3"
    filename.write_text(index)
    reader = CSVIndexReader(filename)
    assert list(reader) == [
        ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
        [
            "101,141,199,66)/robots.txt 20240714155331",
            "cdx-00000.gz",
            "188224",
            "178351",
            "2",
        ],
        ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
    ]
