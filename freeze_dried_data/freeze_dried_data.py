import pickle as pkl
import errno
import os
from typing import Any, Dict, Iterator, Tuple
import zlib
import bz2
import gzip
# files are stored as: record,record,record,...,last record,index,index_length

class FDD:
    """
    A very simple format for machine learning datasets.
    FDD is a simple way to treat datasets on disk like python dictionaries. FDD is built for speed and simplicity.
    Keys can be any objects that are picklable and valid dictionary keys and are stored in memory until the file is closed. Values can be any picklable object, and are stored on disk.
    FDD files can be in either read mode or write mode. Once the file is finalized, it can no longer be modified (i.e., it is "freeze-dried").
    Values are written to disk immediately upon insertion. Keys are written as part of the index when the FDD file is closed.
    """
    def __init__(self, filename: str, write_or_overwrite: bool = False, read_only: bool = False, compression: str = 'none') -> None:
        """
        Initialize a new or existing data file.

        :param filename: The file path used for storing data.
        :param write_or_overwrite: If True, any existing file will be overwritten.
        :param read_only: If True, opens the file in read-only mode and raises FileNotFoundError if the file does not exist.
        """

        self.is_open = False
        self.filename = filename
        self.write_or_overwrite = write_or_overwrite
        self.read_only = read_only
        self.compression = compression
        
        # Compression functions dictionary
        compressors = {
            'zlib': (zlib.compress, zlib.decompress),
            'bz2': (bz2.compress, bz2.decompress),
            'gzip': (gzip.compress, gzip.decompress),
            'none': (lambda x: x, lambda x: x)
        }

        # if compression is a pair of functions
        if isinstance(compression, tuple) and len(compression) == 2 and callable(compression[0]) and callable(compression[1]):
            compressors['custom'] = compression
            compression = 'custom'

        if compression not in compressors:
            raise ValueError(f"Unsupported compression type: {compression}")
        
        self.compressor = compressors[compression][0]
        self.decompressor = compressors[compression][1]

        # Detects when something like PyTorch DataLoader clones the object.
        os.register_at_fork(after_in_child=self._after_fork)

        self._open_file()

    def _open_file(self) -> None:
        """
        Open the file and load the index if in read mode.
        """
        if self.write_or_overwrite and os.path.exists(self.filename):
            os.remove(self.filename)

        if self.read_only and not os.path.exists(self.filename):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), self.filename)
        
        self.mode = 'create_mode' if not os.path.exists(self.filename) else 'read_mode'
        self.file = open(self.filename, 'wb+' if self.mode == 'create_mode' else 'rb+')
        
        self.is_open = True
        self.index = self.get_existing_index() if self.mode == 'read_mode' else {}
        if self.mode == 'create_mode':
            self.current_offset = 0

    def get_existing_index(self) -> Dict[Any, Tuple[int, int]]:
        """
        Retrieve the index map from the end of the file.

        :return: The index dictionary.
        """
        self.file.seek(-8, 2)  # Move to the last 8 bytes for index size
        index_size = int.from_bytes(self.file.read(8), 'little')
        self.file.seek(-(8 + index_size), 2)
        index_data = self.file.read(index_size)
        index = pkl.loads(self.decompressor(index_data))
        return index

    def close(self) -> None:
        """
        Close the file and save the index if in create mode.
        """
        if self.mode == 'create_mode':
            index_data = self.compressor(pkl.dumps(self.index))
            index_data_length = len(index_data)
            self.file.write(index_data)
            self.file.write(index_data_length.to_bytes(8, 'little'))
        
        self.file.close()
        self.is_open = False


    def __setitem__(self, key: Any, item: Any) -> None:
        """
        Add an item to the file with a specific key.

        :param key: The key under which the item will be stored.
        :param item: The item to store.
        :raises ValueError: If the key already exists or trying to insert in read mode.
        """
        if key in self.index:
            raise ValueError("trying to re-insert a record with key,", key, "FDD cannot re-assign items.")
        if self.mode == 'read_mode':
            raise ValueError("trying to insert into a read-mode FDD. This is not supported.", "key=", key)
        data = self.compressor(pkl.dumps(item))
        
        data_len = len(data)
        self.file.write(data)
        self.index[key] = (self.current_offset, data_len)
        self.current_offset += data_len

    def __getitem__(self, key: Any) -> Any:
        """
        Retrieve an item by key.

        :param key: The key of the item to retrieve.
        :return: The item.
        """
        start, data_len = self.index[key]
        self.file.seek(start)
        data = self.file.read(data_len)
        return pkl.loads(self.decompressor(data))

    def _after_fork(self) -> None:
        """
        Reopen the file after a fork to prevent race conditions
        if the object is cloned to another process. For example when using PyTorch DataLoader.
        """
        # print("FDD: Reopening file after fork.")
        self.file.close()
        self.file = open(self.filename, 'rb+')

    def keys(self) -> Iterator[Any]:
        """ Return an iterator over the keys in the file. """
        return self.index.keys()

    def items(self) -> Iterator[Tuple[Any, Any]]:
        """ Yield (key, value) pairs. """
        for k in self.keys():
            yield k, self[k]

    def __contains__(self, item: Any) -> bool:
        """ Check if a key exists in the index. """
        return item in self.index

    def __len__(self) -> int:
        """ Return the number of items in the file. """
        return len(self.index)

    def update(self, dct: Dict[Any, Any]) -> None:
        """
        Update the file with multiple items from a dictionary.

        :param dct: A dictionary of items to add.
        """
        for k, v in dct.items():
            self[k] = v

    def __enter__(self) -> 'FDD':
        """ Support the context manager enter function. """
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """ Support the context manager exit function, closing the file on exit. """
        if self.is_open:
            self.close()

    def __del__(self) -> None:
        """ Ensure the file is closed upon object deletion. """
        if self.is_open:
            self.close()
