import pickle as pkl
import os
from typing import Any, Dict, Iterator, Tuple, Optional, Union, List, Iterable
import sys
import warnings
import zlib
import torch
try:
    from .efficient_index import FDDIndexKeyless, FDDIndexComparableKey, FDDIntList, FDDIndexGeneral, FDDIndexBase
except ImportError:
    from efficient_index import FDDIndexKeyless, FDDIndexComparableKey, FDDIntList, FDDIndexGeneral, FDDIndexBase

type_to_serializer = {
    'any': pkl.dumps,
    'str': lambda x: x.encode('utf-8'),
    'str_compressed': lambda x: zlib.compress(x.encode('utf-8')),
    'bytes': lambda x: x,
    'int128': lambda x: x.to_bytes(16, 'little'),
    'int64': lambda x: x.to_bytes(8, 'little'),
    'int': lambda x: x.to_bytes(8, 'little'),
    'int32': lambda x: x.to_bytes(4, 'little'),
    'int16': lambda x: x.to_bytes(2, 'little'),
    'int8': lambda x: x.to_bytes(1, 'little'),
    'torch': lambda x: pkl.dumps(x.cpu().numpy()),
}

type_to_deserializer = {
    'any': pkl.loads,
    'str': lambda x: x.decode('utf-8'),
    'str_compressed': lambda x: zlib.decompress(x).decode('utf-8'),
    'bytes': lambda x: x,
    'int128': lambda x: int.from_bytes(x, 'little'),
    'int64': lambda x: int.from_bytes(x, 'little'),
    'int': lambda x: int.from_bytes(x, 'little'),
    'int32': lambda x: int.from_bytes(x, 'little'),
    'int16': lambda x: int.from_bytes(x, 'little'),
    'int8': lambda x: int.from_bytes(x, 'little'),
    'torch': lambda x: torch.from_numpy(pkl.loads(x)),
}

class BaseFDD:
    """
    Base class for freeze-dried data. Should not be instantiated directly.
    There are two subclasses: RFDD (for reading) and WFDD (for writing).

    :param filename: The name of the file.
    """

    def __init__(self, filename: str) -> None:
        self.filename = filename

    def __len__(self) -> int:
        return len(self.index)
    
    def __contains__(self, key: Any) -> bool:
        return key in self.index
    
    def keys(self) -> Iterator[Any]:
        return self.index.keys()

    def items(self) -> Iterator[Tuple[Any, 'FDDReadRow']]:
        """ Yield (key, value) pairs, with length awareness for tqdm compatibility. """
        class ItemsWithLength:
            def __init__(self, parent):
                self.parent = parent

            def __iter__(self):
                for k in self.parent.keys():
                    yield k, self.parent[k]

            def __len__(self):
                return len(self.parent.index)

        return ItemsWithLength(self)
    
    def __delattr__(self, name: str) -> None:
        """
        Deletes the custom property with the specified name.

        :param name: The name of the property to delete.
        """
        if name in self.custom_properties:
            del self.custom_properties[name]
            if hasattr(self, 'custom_properties_cache') and name in self.custom_properties_cache:
                del self.custom_properties_cache[name]
        else:
            super().__delattr__(name)

    def __enter__(self) -> 'BaseFDD':
        """
        This method is called when entering the context.
        For example, when using the `with` statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        This method is called when exiting the context.
        For example, when using the `with` statement.
        The close method of the child class is called.
        In the case of WFDD, the index is written to the file.
        """
        self.close()
    
    def close(self) -> None:
        if not self.file.closed:
            self.file.close()


class RFDD(BaseFDD):
    """
    RFDD (Freeze Dried Data Reader) class for reading freeze-dried data files.

    :param filename: The path to the freeze-dried data file.
    :param split: The split name to load, defaults to the all rows split.
    :param system_deserialize: The function to use for deserializing the index, splits, and columns.
        one function for each column.
    """
    def __init__(self,
                 filename: str,
                 split: str = 'all_rows',
                 system_deserialize: callable = pkl.loads) -> None:
        if '^' in filename:
            filename, split = filename.split('^')
        super().__init__(filename)
        self.file = open(filename, 'rb')
        self.system_deserialize = system_deserialize
        self.no_columns_deserialize = pkl.loads
        self.column_to_deserialize = None
        self.columns=None
        
        self.load_indices(split)

        if self.columns is not None and self.column_to_deserialize is None:
            self.column_to_deserialize = tuple(self.system_deserialize for i in range(len(self.columns)))
        
        
        self.custom_properties_cache = {}
        self.read_row_cache = None
        self.cashed_read_row_key = None

        os.register_at_fork(after_in_child=self._after_fork)

    def _after_fork(self) -> None:
        """
        Reopen the file after a fork to prevent race conditions
        if the object is cloned to another process. For example when using PyTorch DataLoader.
        """
        self.file.close()
        self.file = open(self.filename, 'rb+')

    def __getstate__(self) -> object:
        """
        Returns the state of the object for pickling.
        the file object is removed because it cannot be pickled.
        """
        state = self.__dict__.copy()
        state.pop('file')
        return state
    
    def __setstate__(self, state: Dict[str, Any]) -> None:
        """
        Sets the state of the object after unpickling.
        The file object is reopened.
        """
        self.__dict__.update(state)
        self.file = open(self.filename, 'rb+')

    def __getitem__(self, key: Any) -> Union['FDDReadRow', Any]:
        """
        Gets the row with the specified key.

        :param key: The key of the row.
        :return: The row with the specified key. 
        :raises KeyError: If the key is not found.
        """
        
        if key not in self.index:
            if isinstance(key, tuple):
                if key[0] in self.index:
                    indices = self.index[key[0]]
                    col_index = self.columns[key[1]]
                    start = indices[col_index]
                    end = indices[col_index+1]
                    data = self.read_chunk(start, end)
                    return self.column_to_deserialize[col_index](data)
            else:
                raise KeyError("Key not found.", key, self.index.keys())
        
        if self.columns is None:
            
            start, end = self.index[key]
            data = self.read_chunk(start, end)

            return self.no_columns_deserialize(data)
        
        if self.read_row_cache is not None and key == self.cashed_read_row_key:
            return self.read_row_cache
        
        read_row = FDDReadRow(self.index[key], self)
        self.read_row_cache = read_row
        self.cashed_read_row_key = key
        return read_row

    def read_chunk(self, start: int, end: int) -> bytes:
        """
        Reads a chunk of data from the file.

        :param start: The start position of the chunk.
        :param end: The end position of the chunk.
        :return: The data read from the file.
        :rtype: bytes
        """
        self.file.seek(start)
        return self.file.read(end - start)
    
    def _get_split_object(self, split: str) -> FDDIndexBase:
        if split in self.split_to_index:
            start, end = self.split_to_index[split]
            return self.system_deserialize(self.read_chunk(start, end))
        else:
            raise KeyError("Split not found.", split)
        
    def load_new_split(self, split: str) -> None:
        """
        Loads a new split from the file.

        :param split: The name of the split to load.
        """

        if '+' in split:
            splits= split.split('+')
            split_objects = [self._get_split_object(s) for s in splits]
            # make sure they're all the same type
            if not all(type(s) == type(split_objects[0]) for s in split_objects):
                raise ValueError("All splits must be of the same type.")
            if type(split_objects[0]) == FDDIndexComparableKey:
                dct = {}
                for s in split_objects:
                    for k,v in s.items():
                        dct[k] = v
                self.index = FDDIndexComparableKey(dct)
            elif type(split_objects[0]) == FDDIndexGeneral:
                self.index = split_objects[0]
                for s in split_objects[1:]:
                    for k,v in s.items():
                        self.index[k] = v

            elif type(split_objects[0]) == FDDIndexKeyless:
                rows = {}
                for s in split_objects:
                    for v in s.values():
                        rows[tuple([i for i in v])] = True
                self.index = FDDIndexKeyless(split_objects[0].num_vals, split_objects[0].byte_width)
                for i,r in enumerate(rows.keys()):
                    # print(i,r, len(self.index.buffer), len(self.index))
                    # print(len(self.index.buffer), self.index.byte_width,self.index.num_vals)
                    self.index[i] = r
        else:
            self.index = self._get_split_object(split)

    def get_available_splits(self) -> List[str]:
        """
        :return: A list of available splits.
        """
        return list(self.split_to_index.keys())
        

    def load_indices(self, split: str = 'all_rows') -> None:
        """
        Reads from the end of the file loading custom properties, columns, and the split index.

        :param split: The split to load, defaults to 
        """
        self.file.seek(-8, 2)
        index_index_size = int.from_bytes(self.file.read(8), 'little')

        self.file.seek(-(8 + index_index_size), 2)
        index_index_data = self.file.read(index_index_size)
        index_index = self.system_deserialize(index_index_data)
        
        self.split_to_index = {k[7:]:v for k,v in index_index.items() if k.startswith('_split_')}
        

        # remove splits from index_index
        for k in self.split_to_index:
            index_index.pop('_split_'+k)

        # index_start, index_end = self.split_to_index[split]
        
        # self.index = self.system_deserialize(self.read_chunk(index_start, index_end))
        self.load_new_split(split)
        
        if '_columns_' not in index_index:
            self.columns = None
        else:
            columns_start, columns_end = index_index['_columns_']
            index_index.pop('_columns_')

            self.columns = self.system_deserialize(self.read_chunk(columns_start, columns_end))
            if self.columns is not None:
                self.columns = {n: i for i, n in enumerate(self.columns)}
        
        if '_column_def_' in index_index:
            column_def_start, column_def_end = index_index['_column_def_']
            index_index.pop('_column_def_')
            try:
                self.column_def = self.system_deserialize(self.read_chunk(column_def_start, column_def_end))
            except Exception as e:
                import dill
                self.column_def = dill.loads(self.read_chunk(column_def_start, column_def_end))

            self.column_to_deserialize = {v: type_to_deserializer[t] if isinstance(t, str) else t[1] for v, t in self.column_def.items()}   
            self.column_to_deserialize = tuple(self.column_to_deserialize.values())
                

        self.custom_properties = {k[6:]:v for k,v in index_index.items() if k.startswith('_prop_')}


    def __getattr__(self, name: str) -> Any:
        """
        Gets the custom property with the specified name.

        :param name: The name of the custom property.
        :return: The value of the custom property.
        :raises AttributeError: If the custom property is not found.
        """
        if name in self.custom_properties_cache:
            return self.custom_properties_cache[name]
        if name in self.custom_properties:
            bytes = self.read_chunk(*self.custom_properties[name])
            loaded_prop = self.system_deserialize(bytes) 
            self.custom_properties_cache[name] = loaded_prop
            return loaded_prop
        else:
            return super().__getattr__(name)

class FDDReadRow:
    """
    This class enables lazy loading of properties from rows.
    It also allows read rows to be modified before writing them to a new FDD.
    Writing unmodified rows to a new WFDD is fast because the data is not deserialized.

    :param index: A list of locations in the file where the row data is stored.
    :param parent: The parent FDD table. This could be a WFDD or RFDD instance.
    """

    def __init__(self, index: Tuple[Any], parent, ) -> None:
        self._fdd_row_index = index
        self._fdd_row_parent = parent
        self._fdd_row_cache = [None for _ in range(len(self._fdd_row_index) - 1)]

    def as_dict(self):
        """
        :return: A dictionary representation of the row.
        """
        return {k: self[i] for i, k in enumerate(self._fdd_row_parent.columns)}
    
    def __getitem__(self, key: int | str) -> Any:
        """
        Gets the value of the column at the specified index or with the specified name.

        :param key: The index of the column, or the name of the column.
        :return: The value of the column.
        """
        if isinstance(key, str):
            key = self._fdd_row_parent.columns[key]
        if self._fdd_row_cache[key] is None:
            start = self._fdd_row_index[key]
            end = self._fdd_row_index[key+1]
            if start == end:
                self._fdd_row_cache[key] = None
                # alternatively, we could raise an error here
            else:
                deserialize = self._fdd_row_parent.column_to_deserialize[key]
                self._fdd_row_cache[key] = deserialize(self._fdd_row_parent.read_chunk(start, end))
            

        return self._fdd_row_cache[key]
    
    def __setitem__(self, key: int | str, value: Any) -> None:
        """
        Sets the value of the column at the specified index.

        :param key: The index of the column.
        :param value: The value to set.
        """
        if isinstance(key, str):
            key = self._fdd_row_parent.columns[key]
        self._fdd_row_cache[key] = value

    def __getattr__(self, name: str) -> Any:
        """
        Gets the value of the column with the specified name.

        :param name: The name of the column.
        :return: The value of the column.
        :raises AttributeError: If the column is not found.
        """
        if name.startswith('_fdd_row_'):
            return super().__getattr__(name)
        
        columns: tuple | dict = self._fdd_row_parent.columns

        if name not in columns:
            raise AttributeError(f"Column not found: {name}")
        
        index = columns[name] if isinstance(columns, dict) else columns.index(name)
        
        return self[index]
    
    def __setattr__(self, name: str, value: Any) -> None:
        """
        Sets the value of the column with the specified name.

        :param name: The name of the column.
        :param value: The value to set.
        :raises AttributeError: If the column is not found.
        """
        if name.startswith('_fdd_row_'):
            super().__setattr__(name, value)
            return
        if isinstance(self._fdd_row_parent, WFDD):
            raise AttributeError("Row has already been finalized.")
        columns = self._fdd_row_parent.columns
        if name not in columns:
            raise AttributeError(f"Column not found: {name}")
        
        index = columns[name] if isinstance(columns, dict) else columns.index(name)
        self._fdd_row_cache[index] = value

    def __contains__(self, key: str) -> bool:
        """
        Checks if the row contains the specified column.

        :param key: The name of the column.
        :return: True if the column is found, False otherwise.
        """
        return key in self._fdd_row_parent.columns
    
    def __iter__(self) -> Iterator[Any]:
        """
        Yields the values for the row.
        """
        for i, key in enumerate(self._fdd_row_parent.columns):
            yield key
    
    def __hasattr__(self, name: str) -> bool:
        """
        Checks if the row contains the specified column.

        :param name: The name of the column.
        :return: True if the column is found, False otherwise.
        """
        return name in self._fdd_row_parent.columns
    
    def items(self) -> Iterator[Tuple[str, Any]]:
        """
        Yields (column name, value) pairs for the row.
        """
        for i, key in enumerate(self._fdd_row_parent.columns):
            yield key, self[i]

    def keys(self) -> Iterator[str]:
        """
        Yields the column names for the row.
        """
        for key in self._fdd_row_parent.columns:
            yield key
    
    def values(self) -> Iterator[Any]:
        """
        Yields the values for the row.
        """
        for i, key in enumerate(self._fdd_row_parent.columns):
            yield self[i]

    
    
    def __repr__(self) -> str:
        """
        :return: A human readable string representation of the row. One line per column.
        """
        rep = ""
        for i, key in enumerate(self._fdd_row_parent.columns):
            value = self[i]
            rep += f"{key}: {value}\n"
        return rep

class WFDD(BaseFDD):
    """
    WFDD (Freeze Dried Data Writer) class for writing freeze-dried data files.

    :param filename: The path to the freeze-dried data file.
    :param columns: The column names for the data. If None, columns won't be used to store the data.
    :param overwrite: Whether to overwrite an existing file, default is False.
    :param reopen: Whether to reopen an existing file, default is False.
    :param system_serialize: The function to use for serializing the index, splits, and columns.
    

    """
    def __init__(self,
                 filename: str,
                 columns: dict[str, str | tuple[callable, callable]] = None,
                 overwrite: bool = False,
                 reopen: bool = False,
                 system_serialize: callable = pkl.dumps,
                 system_deserialize: callable = pkl.loads,
                 ) -> None:
        self.__dict__['_initializing'] = True # Use self.__dict__ to bypass __setattr__
        super().__init__(filename)
        
        if not overwrite and not reopen and os.path.exists(filename):
            raise FileExistsError("File already exists.", filename)
        
        if reopen and not os.path.exists(filename):
            raise FileNotFoundError("File not found.", filename)
        
        column_to_serialize = {v: type_to_serializer[t] if isinstance(t, str) else t[0] for v, t in columns.items()} if columns is not None else None
        
        column_to_deserialize = {v: type_to_deserializer[t] if isinstance(t, str) else t[1] for v, t in columns.items()} if columns is not None else None
        
        self.column_def = columns

        if columns is not None:
            self.column_to_deserialize = tuple(column_to_deserialize.values())
            self.column_to_serialize = tuple(column_to_serialize.values())
        else:
            self.column_to_deserialize = None
            self.column_to_serialize = None
        
        
        self.system_serialize = system_serialize
        self.system_deserialize = system_deserialize
        self.no_columns_serialize = pkl.dumps
        self.no_columns_deserialize = pkl.loads

        if reopen:
            self.reopen()
        else:
            self.file = open(filename, 'wb+')
            self.unfinished_setters = {}
            
            self.columns = tuple(columns.keys()) if columns is not None else None
            self.index = FDDIndexGeneral(len(columns)+1 if columns is not None else 2)
            self.custom_properties = {}
            self.split_to_index = {}
        
        self._initializing = False

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Set an attribute value.

        :param name: The name of the attribute.
        :param value: The value to set.
        """
        if self._initializing or name in self.__dict__ or name in type(self).__dict__:
            super().__setattr__(name, value)
        else:
            self.custom_properties[name] = value

    def __getattr__(self, name: str) -> Any:
        """
        Get an attribute value.

        :param name: The name of the attribute.
        :return: The value of the attribute.
        """
        if name in self.custom_properties:
            return self.custom_properties[name]
        else:
            return super().__getattr__(name)

    def read_chunk(self, start, end) -> bytes:
        """
        Read a chunk of data from the file.

        :param start: The start position of the chunk.
        :param end: The end position of the chunk.
        :return: The data read from the file.
        """
        pos = self.file.tell()
        self.file.seek(start)
        data = self.file.read(end - start)
        self.file.seek(pos)
        return data

    def __getitem__(self, key: Any) -> Union[Any, 'FDDSetter']:
        """
        Get an item from the WFDD.

        :param key: The key of the item.
        :return: The item corresponding to the key.
        """
        if key not in self.index:
            if self.columns is None:
                raise KeyError("Key not found.", key)
            if key not in self.unfinished_setters:
                self.unfinished_setters[key] = FDDSetter(self, key)
            return self.unfinished_setters[key]
        else:
            if self.columns is None:
                start,end = self.index[key]
                data = self.read_chunk(start, end)
                return self.no_columns_deserialize(data)
        
        pos = self.file.tell()
        #TODO: investigate, I think read-row needs to know where to seek after it does the reading.
        #the tests do check to make sure this works though.
        row = FDDReadRow(self.index[key], self) 
        self.file.seek(pos)
        return row
        
    def __setitem__(self, key: Any, item: dict[str, Any] | tuple[Any] | FDDReadRow | Any) -> None:
        """
        Set an item in the WFDD.

        :param key: The key of the item.
        :param item: The item to set.
        """
        if key in self.index:
            raise KeyError("Key already exists.", key)
        
        if self.columns is None:
            item = (item,)
        elif isinstance(item, FDDReadRow):
            positions = [self.file.tell()]
            for i in range(len(self.columns)):
                # if it's in cache, serialize it and write it, otherwise, write the raw bytes
                if item._fdd_row_cache[i] is not None:
                    serialize = self.column_to_serialize[i]
                    data = serialize(item._fdd_row_cache[i])
                else:
                    data = item._fdd_row_parent.read_chunk(item._fdd_row_index[i], item._fdd_row_index[i+1])
                self.file.write(data)
                positions.append(self.file.tell())
            self.index[key] = tuple(positions)
            return
        elif isinstance(item, dict):
            if all(col not in item for col in self.columns):
                raise ValueError("Dict contains no entries for columns.", [col for col in self.columns if col not in item])
            if any(col not in self.columns for col in item):
                raise ValueError("Dict contains columns not in the column list.", [col for col in item if col not in self.columns])
            item = tuple(item.get(col) for col in self.columns)
        elif isinstance(item, tuple):
            if len(item) != len(self.columns):
                raise ValueError("Incorrect number of columns.")
            #if it has all attributes in our columns
        elif all(hasattr(item, col) for col in self.columns):
            item = tuple(getattr(item, col) for col in self.columns)
        else:
            raise ValueError("Invalid type for column mode. Must be dict, tuple, or object with attributes matching the columns.")
        
        positions = [self.file.tell()]
        for i, v in enumerate(item):
            if v is not None:
                if self.columns is not None:
                    serialize = self.column_to_serialize[i]
                else:
                    serialize = self.no_columns_serialize
                data = serialize(v)
                self.file.write(data)
            positions.append(self.file.tell())
            
        self.index[key] = tuple(positions)    

    def make_split(self, split: str, rows: Iterable, overwrite=False, keyless=False, preserve_order=True) -> None:
        """
        Create a split in the WFDD.

        :param split: The name of the split.
        :param rows: The iterable of keys for the split.
        """
        if split in self.split_to_index and not overwrite:
            raise ValueError("Split already exists.", split, 'Use overwrite=True to overwrite it.')
        
        if not isinstance(rows, str):
            
            if keyless:
                split_index = FDDIndexKeyless(num_vals=len(self.columns)+1 if self.columns is not None else 2)
                for i, key in enumerate(rows):
                    split_index[i] = self.index[key]
            else:
                split_index_dict = {key: self.index[key] for key in rows}
                try:
                    if not preserve_order:
                        split_index = FDDIndexComparableKey(split_index_dict)
                    else:
                        raise ValueError("dummy error go to except block")
                except:
                    split_index = FDDIndexGeneral(len(self.columns)+1 if self.columns is not None else 2)
                    for k,v in split_index_dict.items():
                        split_index[k] = v
            if split == 'all_rows':
                self.index = split_index
            else:
                self.split_to_index[split] = split_index
        else:
            raise ValueError("Rows must be an iterable of keys.")
        
    def add_to_split(self, split: str, rows: list | tuple | set | frozenset) -> None:
        """
        Add rows to a split in the WFDD.

        :param split: The name of the split.
        :param rows: The iterable of keys for the split.
        """
        if split not in self.split_to_index:
            raise ValueError("Split not found.", split)
        
        if isinstance(rows, (list, tuple, set, frozenset)):
            self.split_to_index[split].update({key: self.index[key] for key in rows})
        else:
            raise ValueError("Rows must be an iterable of keys.")
        
    def reopen(self) -> None:
        self.file = open(self.filename, 'rb+')
        self.file.seek(-8, 2)
        index_index_size = int.from_bytes(self.file.read(8), 'little')

        self.file.seek(-(8 + index_index_size), 2)
        earliest = self.file.tell()
        index_index_data = self.file.read(index_index_size)
        index_index = self.system_deserialize(index_index_data)
        
        self.split_to_index = {k[7:]:v for k,v in index_index.items() if k.startswith('_split_')}
        

        # remove splits from index_index
        for k in self.split_to_index:
            index_index.pop('_split_'+k)

        index_start, index_end = self.split_to_index["all_rows"]
        earliest = min(index_start,earliest)
        
        self.index = self.system_deserialize(self.read_chunk(index_start, index_end))

        new_split_to_index = {}
        for k,v in self.split_to_index.items():
            if k == "all_rows":
                continue
            split_start, split_end = v
            earliest = min(split_start,earliest)
            new_split_to_index[k] = self.system_deserialize(self.read_chunk(split_start, split_end))

        self.split_to_index = new_split_to_index
        
        if '_columns_' not in index_index:
            self.columns = None
        else:
            columns_start, columns_end = index_index['_columns_']
            earliest = min(columns_start, earliest)
            index_index.pop('_columns_')

            self.columns = self.system_deserialize(self.read_chunk(columns_start, columns_end))
            
        self.custom_properties = {k[6:]:v for k,v in index_index.items() if k.startswith('_prop_')}
        
        # need to figure out where to seek so that we are at the end of the rows. Everything else shoudl be in ram.
        new_custom_properties = {}
        for k,v in self.custom_properties.items():
            prop_start, prop_end = v
            if prop_start < earliest:
                earliest = prop_start
            new_custom_properties[k] = self.system_deserialize(self.read_chunk(prop_start, prop_end))
        self.custom_properties = new_custom_properties
        

        self.unfinished_setters = {}

        self.file.seek(earliest)

    def close(self) -> None:
        """
        Close the WFDD and write data to disk.
        """
        # write out all unfinished setters
        cpy = list(self.unfinished_setters.keys())
        if len(cpy) > 1000:
            warnings.warn("""
            Warning: You have a large number of unfinished setters.
            It is recommended to call finalize() as soon as you are done processing a row.
            Unfinished rows are kept in memory until close() is called.
            For a WFDD with columns ('a', 'b', 'c'), you can call finalize() like this:
            wfdd[key1].a = 1 # doesn't set b or c
            wfdd[key1].finalize() # we're done with this row now, it will be written to disk
            finalize() will be called automatically when you have set all columns in a row or when the WFDD is closed.
            
            Writing {} unfinished setters to disk now.
            """.format(len(cpy)), UserWarning)

        for k in cpy:
            self.unfinished_setters[k].finalize()

        index_index = {}
        has_lambda = False
        if self.column_def is not None:
            for k,v in self.column_def.items():
                if isinstance(v, tuple):
                    has_lambda = True
                    break
        
        if self.column_def is not None:
            column_def_start = self.file.tell()
            if not has_lambda:
                column_def_data = self.system_serialize(self.column_def)
            else:
                # use dill
                import dill
                column_def_data = dill.dumps(self.column_def)

            self.file.write(column_def_data)
            column_def_end = self.file.tell()
            index_index['_column_def_'] = (column_def_start, column_def_end)



        for k,v in self.custom_properties.items():
            property_start = self.file.tell()
            property_data = self.system_serialize(v)
            self.file.write(property_data)
            property_end = self.file.tell()
            index_index["_prop_"+k] = (property_start, property_end)

        self.split_to_index['all_rows'] = self.index
        for k,v in self.split_to_index.items():
            split_start = self.file.tell()
            split_data = self.system_serialize(v)
            self.file.write(split_data)
            split_end = self.file.tell()
            index_index["_split_"+k] = (split_start, split_end)

        if self.columns is not None:
            columns_start = self.file.tell()
            columns_data = self.system_serialize(self.columns)
            self.file.write(columns_data)
            columns_end = self.file.tell()
            index_index['_columns_'] = (columns_start, columns_end)

        index_index_data = self.system_serialize(index_index)
        index_index_data_length = len(index_index_data)

        self.file.write(index_index_data)
        self.file.write(index_index_data_length.to_bytes(8, 'little'))
        self.file.flush()
        super().close()

class FDDSetter:
    """
    Represents a setter object used to set data for a specific key in a parent object.
    This class allows for setting row properties by name, i.e. `wfdd[key].a = 1`.
    Once all attributes are set, finalize() is called, or the WFDD is closed,
    the data is written to disk and the FDDSetter is removed from the parent object.

    :param parent: The parent object that holds the data.
    :param key: The key used to identify the data in the parent object.
    """
    def __init__(self, parent: 'WFDD', key: Any) -> None:
        self._fdd_setter_parent = parent
        self._fdd_setter_key = key
        self._fdd_setter_data = {}
        self._fdd_setter_finalized = False

    def finalize(self) -> None:
        """
        Finalizes the data setting process by updating the parent object and removing the setter from the unfinished setters list.
        """
        self._fdd_setter_parent[self._fdd_setter_key] = self._fdd_setter_data
        del self._fdd_setter_parent.unfinished_setters[self._fdd_setter_key]
        self._fdd_setter_finalized = True
    
    def __setattr__(self, name: str, value: Any) -> None:
        """
        Sets the appropriate column in the row. Coluns aren't written to disk until the row is finalized.
        Rows are finalized when all columns are set, when finalize() is called, or when the WFDD is closed.


        """
        if name.startswith('_fdd_setter_'):
            super().__setattr__(name, value)
            return
        
        if self._fdd_setter_finalized:
            raise AttributeError("Row has already been finalized.")
        
        columns = self._fdd_setter_parent.columns
        if name not in columns:
            raise AttributeError(f"Column not found: {name}")
        
        self._fdd_setter_data[name] = value
        if len(self._fdd_setter_data) == len(self._fdd_setter_parent.columns):
            self.finalize()

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Sets the value of the column with the specified name.

        :param key: The name of the column.
        :param value: The value to set.
        """
        self.__setattr__(key, value)


def add_column(input_path, output_path, column_name, column_data, overwrite=False, column_type='any'):
    """
    Add a column to a freeze-dried data file.

    :param input_path: The path to the input freeze-dried data file.
    :param output_path: The path to the output freeze-dried data file.
    :param column_name: The name of the column to add.
    :param column_data: The data for the column.
    """
    
    # if it has an items() funciton, call it
    if hasattr(column_data, 'items'):
        column_data = column_data.items()

    with RFDD(input_path) as rfdd:
        if column_name in rfdd.columns:
            raise ValueError("Column already exists.", column_name)
        
        column_def = rfdd.column_def
        column_def[column_name] = column_type

        column_serialize = type_to_serializer[column_type]

        
        with WFDD(output_path, columns=column_def, overwrite=overwrite) as wfdd:
            
            for key, value in column_data:
                row_index = rfdd.index[key]
                start, end = row_index[0], row_index[-1]

                row_data = rfdd.read_chunk(start, end)

                new_data_for_row = column_serialize(value)
                current_index = list(row_index)
                current_index.append(end+len(new_data_for_row))

                start = wfdd.file.tell()

                current_index = [i-current_index[0]+start for i in current_index]

                wfdd.index[key] = tuple(current_index)

                row_data+=new_data_for_row
                wfdd.file.write(row_data)

            # copy the splits
            rfdd.get_available_splits()
            for split in rfdd.get_available_splits():
                rfdd.load_new_split(split)
                rows = list(rfdd.index.keys())
                wfdd.make_split(split, rows)

            for k in rfdd.custom_properties.keys():
                v = rfdd.__getattr__(k)
                wfdd.__setattr__(k, v)


