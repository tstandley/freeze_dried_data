# Freeze Dried Data
A very simple format for machine learning datasets.


FDD is a simple way to treat datasets on disk like python dictionaries.
FDD is extremely fast and is implemented as a single ~100 line python file.

Keys can be any objects that are picklable and valid dictionary keys and
are stored in memory until the file is closed. Values can be any picklable
object, and are stored on disk.

FDD files can be in either read mode or write mode. Once the file is finalized,
it can no longer be modified (i.e., it is "freeze-dried").

Values are written to disk immediately upon insertion. Keys are written
as part of the index when the FDD file is closed.

## Installation (PyPI)
```bash
pip install freeze-dried-data
```
## Examples

### Example 1: Creating an FDD file
```python
from freeze_dried_data import FDD

# creates the file "new dataset.fdd" if it does not exist
dataset = FDD('new dataset.fdd')

# adds the entry 'key1': 'value1' to the dataset and writes the value to disk
dataset['key1'] = 'value1'

# adds the entry 1234: 5678 to the dataset and writes the value to disk
dataset[1234] = 5678

# writes the index including all keys to disk and closes the file.
# FDD files are automatically closed upon __exit__() or __del__()
dataset.close()
```


### Example 2: Reading an FDD file
```python
from freeze_dried_data import FDD

# opens the existing file. Unpickles the index including all keys into memory.
dataset = FDD('new dataset.fdd')

# prints:
# key1 value1
# 1234 5678
for k, v in dataset.items():
  print(k, v)

# prints "5678"
print(dataset[1234])

# prints "['key1', 1234]"
print(list(dataset.keys()))
```

### Example 3: With syntax and explicit mode setting
```python
from freeze_dried_data import FDD

# opens a new fdd for writing using the with syntax. If the file already
# exists, it will be overwritten.
with FDD('new dataset.fdd', write_or_overwrite=True) as new_dataset:
  new_dataset['test_key'] = 'test_val'
# file is automatically closed and written out at the end of the with block.

# opens the FDD file for reading. If the file does not exist, an error is thrown.
# without the `read_only=True` parameter, an empty file is created if the file
# does not exist, and the empty file is opened for writing.
with FDD('new dataset.fdd', read_only=True) as new_dataset:
  print(new_dataset['test_key']) # prints "test_val"

```
