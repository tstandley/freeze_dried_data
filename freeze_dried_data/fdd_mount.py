from freeze_dried_data import RFDD
from fuse import FUSE, Operations, FuseOSError
from collections import OrderedDict, defaultdict
import threading
import time
import os
import sys
import errno
import argparse
from typing import Callable, Optional, Any, Dict


class ByteSizeLRUCache:
    """
    An LRU cache that evicts entries based on total byte size.
    Thread-safe for use in multithreaded FUSE environments.
    """
    def __init__(self, max_bytes: int):
        self.max_bytes = max_bytes
        self.cache = OrderedDict()  # key -> content (bytes)
        self.total_size = 0
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[bytes]:
        with self.lock:
            if key not in self.cache:
                return None
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value: bytes):
        size = len(value)
        with self.lock:
            if key in self.cache:
                self.total_size -= len(self.cache[key])
                self.cache.move_to_end(key)
            self.cache[key] = value
            self.total_size += size

            while self.total_size > self.max_bytes:
                _, old_value = self.cache.popitem(last=False)
                self.total_size -= len(old_value)


class FddFiles(Operations):
    """
    A read-only FUSE filesystem backed by a Freeze Dried Data (RFDD) file.
    Each row becomes a virtual file with a lazily generated name and content.
    Supports nested directories based on the output of name_lambda(row).
    """
    def __init__(
        self,
        fdd: RFDD,
        name_lambda: Callable[[Any], str],
        content_lambda: Callable[[Any], bytes],
        size_lambda: Optional[Callable[[Any], int]] = None,
        cache_size_bytes: int = 100 * 1024 * 1024,
    ):
        """
        fdd: an RFDD instance
        name_lambda: function(row) -> str
        content_lambda: function(row) -> str or bytes
        size_lambda: optional function(row) -> int
        cache_size_bytes: max size of content cache in bytes
        """
        self.fdd = fdd
        self.name_lambda = name_lambda
        self.content_lambda = content_lambda
        self.size_lambda = size_lambda  # Optional
        self.start_time = time.time()
        self.file_map = {}  # path -> row_key
        for row_key, row in fdd:
            name = "/" + name_lambda(row)
            self.file_map[name] = row_key

        self._content_cache = ByteSizeLRUCache(cache_size_bytes)

        self.file_map: Dict[str, Any] = {}      # full path → row_key
        self.dir_map: Dict[str, set] = defaultdict(set)  # dir path → child names

        for row_key, row in fdd:
            rel_path = name_lambda(row).strip("/")
            full_path = "/" + rel_path
            self.file_map[full_path] = row_key

            # Register all parent directories
            parts = rel_path.split("/")
            for i in range(len(parts)):
                parent = "/" + "/".join(parts[:i])
                child = parts[i]
                self.dir_map[parent].add(child)

    def _now(self):
        return self.start_time

    def getattr(self, path: str, fh=None) -> dict:
        import stat

        if path == "/" or path in self.dir_map:
            return {
                "st_mode": stat.S_IFDIR | 0o755,
                "st_nlink": 2,
                "st_ctime": self.start_time,
                "st_mtime": self.start_time,
                "st_atime": self.start_time,
            }

        row_key = self.file_map.get(path)
        if row_key is not None:
            row = self.fdd[row_key]
            size = self.size_lambda(row) if self.size_lambda else self._get_size_from_cache_or_load(row_key, row)
            return {
                "st_mode": stat.S_IFREG | 0o444,
                "st_nlink": 1,
                "st_size": size,
                "st_ctime": self.start_time,
                "st_mtime": self.start_time,
                "st_atime": self.start_time,
            }

        # raise FileNotFoundError(f"File or directory not found: {path}")
        raise FuseOSError(errno.ENOENT)

    def _get_size_from_cache_or_load(self, row_key: str, row: Any) -> int:
        content = self._content_cache.get(row_key)
        if content is None:
            content = self.content_lambda(row)
            if isinstance(content, str):
                content = content.encode("utf-8")
            self._content_cache.put(row_key, content)
        return len(content)

    def readdir(self, path: str, fh):
        if path not in self.dir_map:
            # raise FileNotFoundError(f"Directory not found: {path}")
            raise FuseOSError(errno.ENOTDIR)
        return [".", ".."] + sorted(self.dir_map[path])

    def open(self, path: str, flags):
        if path not in self.file_map:
            # raise FileNotFoundError(f"File not found: {path}")
            raise FuseOSError(errno.ENOENT)
        return 0

    def read(self, path: str, size: int, offset: int, fh):
        row_key = self.file_map.get(path)
        if row_key is None:
            # raise FileNotFoundError(f"File not found: {path}")
            raise FuseOSError(errno.ENOENT)

        content = self._content_cache.get(row_key)
        if content is None:
            row = self.fdd[row_key]
            content = self.content_lambda(row)
            if isinstance(content, str):
                content = content.encode("utf-8")
            self._content_cache.put(row_key, content)

        return content[offset:offset + size]


def main():
    parser = argparse.ArgumentParser(
        description="Mount an RFDD file as a virtual read-only filesystem."
    )
    parser.add_argument("rfdd_path", help="Path to the .rfdd file")
    parser.add_argument("mountpoint", help="Mount point for the FUSE filesystem")

    # Lambda overrides (optional)
    parser.add_argument("--name-lambda", default="", help="Override lambda for filename generation")
    parser.add_argument("--content-lambda", default="", help="Override lambda for file content")
    parser.add_argument("--size-lambda", default="", help="Optional override lambda for file size")

    # Column/extension config
    parser.add_argument("--filename-column", default="filename", help="Column name for file names (default: 'filename')")
    parser.add_argument("--content-column", default="content", help="Column name for file contents (default: 'content')")
    parser.add_argument("--filename-extension", default="", help="Extension to add to file names (e.g. .txt)")

    # Cache config
    parser.add_argument("--cache-size", type=int, default=100, help="Cache size in megabytes (default: 100)")

    args = parser.parse_args()

    try:
        rfdd = RFDD(args.rfdd_path)
    except Exception as e:
        print(f"Error loading RFDD file: {e}")
        sys.exit(1)

    # Validate column types if lambdas not provided
    if not args.name_lambda:
        name_col_type = rfdd.column_def.get(args.filename_column)
        if name_col_type != "str":
            print(f"Error: filename column '{args.filename_column}' must be type 'str', got '{name_col_type}'")
            sys.exit(1)

    if not args.content_lambda:
        content_col_type = rfdd.column_def.get(args.content_column)
        if content_col_type not in ("str", "bytes"):
            print(f"Error: content column '{args.content_column}' must be 'str' or 'bytes', got '{content_col_type}'")
            sys.exit(1)

    # Construct lambdas
    try:
        if args.name_lambda:
            name_lambda = eval(args.name_lambda)
        else:
            def name_lambda(row): return row[args.filename_column] + args.filename_extension

        if args.content_lambda:
            content_lambda = eval(args.content_lambda)
        else:
            def content_lambda(row):
                val = row[args.content_column]
                return val.encode("utf-8") if isinstance(val, str) else val

        if args.size_lambda:
            size_lambda = eval(args.size_lambda)
        else:
            def size_lambda(row):
                col_idx = row._fdd_row_parent.columns[args.content_column]
                return row._fdd_row_index[col_idx + 1] - row._fdd_row_index[col_idx]


    except Exception as e:
        print(f"Error evaluating lambda: {e}")
        sys.exit(1)

    # Initialize FUSE filesystem
    fs = FddFiles(
        fdd=rfdd,
        name_lambda=name_lambda,
        content_lambda=content_lambda,
        size_lambda=size_lambda,
        cache_size_bytes=args.cache_size * 1024 * 1024
    )

    os.makedirs(args.mountpoint, exist_ok=True)

    print(f"Mounting {args.rfdd_path} at {args.mountpoint} (cache: {args.cache_size} MB)")
    try:
        FUSE(fs, args.mountpoint, foreground=True, ro=True, allow_other=True, nothreads=False)
    except RuntimeError as e:
        print(f"FUSE mount failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
