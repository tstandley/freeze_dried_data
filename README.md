# Freeze Dried Data
A format for machine learning datasets.

FDD allows your entire dataset to be a single file while supporting fast random access to records on disk.

Work with FDDs like you would python dictionaries. Keys map to objects. For extra organization and speed, columns can be defined such that every row has a value for each column. 

FDD files can operate in either read mode or write mode. With few exceptions, once a file is finalized, it cannot be modified without being re-written (i.e., it is "freeze-dried"). This allows for increased simplicity and speed compared to databases that allow modification.

Values are written to disk immediately upon insertion, while keys are written as part of the index when the FDD file is closed.

## Installation
```bash
pip install freeze-dried-data
```
Alternatively, copy the whole `freeze_dried_data/` package folder (it needs both `freeze_dried_data.py` and `efficient_index.py`) into your project directory.

Requires Python 3.10+. Custom column serializers are stored with [`dill`](https://pypi.org/project/dill/), which is installed as a dependency.

## Features

### Custom Splits
Define custom splits in your dataset by storing a list of keys for each split. This allows you to easily manage and access different subsets of your dataset, such as train, val, and test sets.

If you key by hash, storing your train, val, and test sets in the same file can reduce the chance of having train/test or train/val overlap.

Custom splits can also maintain order.

Splits also take the place of sharding in many cases. With one split per shard, only the index for the selected split is stored in memory. Everything else can remain on disk. And everything can still remain in a single file.

Splits can be loaded as part of the path with the ^ separator. This allows flags for which file to load to also specify which split.

Using the + signifier in the split specification, the union of two or more splits can be loaded (i.e. split='train+val' will load the union of the train and val splits)

### Performance Features
- **Selective Data Loading**: The data for a row's column is only loaded from disk when accessed. This allows for dataloaders to ignore certain columns and not incur a performance penalty by loading them from disk.

- **Fast Modified Copying**: When processing a .fdd into a new, modified file, one typically loops through the records in the original file, makes the desired modification/addition to each row, then writes that row out to the new file. FDD directly copies the bits for columns that are unmodified so that no unnecessary serialization/deserialization needs to take place.

- **Keyless Splits**: FDD allows keyless splits and indices.
  ```
  wfdd.make_split(split_name,keys_in_split,keyless=True)
  ```
  Because they do not keep keys in memory, nor write them to disk, keyless splits are much more CPU, memory, and disk space efficient. Unlike regular splits, the index of a keyless split is never even loaded to memory. With keyless splits, FDD files load almost instantly in addition to requiring very little RAM.

  When a keyless split is loaded, rows can be accessed by integer from 0 to len(rfdd).
  ```
  rfdd[0] # returns the first row in the FDD file.
  ```

  Using ```load_keys()``` with a custom function that defines the key for each row, keys can be restored so that rows can once again be accessed in random order by key.
 
### Custom Serialization and Deserialization
FDD allows custom functions for serializing and deserializing data. This flexibility is especially useful when dealing with complex data types or when performance optimizations are necessary. It also allows on-the-fly compression where the compression algorithm can be chosen on a per-column basis. Finally, custom serialization can be much more efficient when storing tensors (numpy, pytorch, etc.)

Custom serializers/deserializers are stored with ```dill``` in the `.fdd` file so that they do not have to be respecified when loaded.

Portability depends on where the functions are defined. Functions imported from a module are stored by reference, so that module must be importable wherever the file is read. Functions defined in `__main__` are stored by value, but the names they use from their global scope (imports, helpers) are not stored. Put the imports inside the functions (see Example 7) so the file can be read from any process.

### Custom Properties
Custom properties are a great place to store dataset metadata such as dataset cards or the code/parameters used to generate the data. In read mode, properties are loaded from disk only when accessed, so this need not incur a runtime cost.

### Seamless Integration
FDD is designed to work with data loaders in machine learning frameworks like PyTorch. Unlike other solutions, RFDD objects detect when they've been forked to a new process and re-open their file handles.

Caveats:
- This relies on the `fork` start method (the Linux default). An RFDD for a file with columns cannot be pickled with the standard `pickle` module (the built-in column codecs are lambdas), so `spawn`/`forkserver` contexts fail; open the RFDD lazily inside each worker instead. Combined RFDDs (`'a.fdd,b.fdd'`) cannot be pickled at all.
- RFDD objects are not thread-safe (reads share one file handle). Give each thread its own RFDD.

### Context Management
FDD supports Python’s context management (using `with` statements), which ensures that files are properly closed after operations are completed, preventing data corruption and resource leaks.

### Easy Appending to Existing Files
FDD supports reopening existing files for writing. Reopened files retain all rows, custom properties, and splits already added to the file, while allowing new rows, properties, or splits to be written. Splits can also be modified or replaced with `make_split(..., overwrite=True)`. `add_to_split()` works on keyed splits only; rebuild keyless splits with `make_split`.

Reopening with a new `columns=` definition (same number and order of columns) renames columns or swaps their decoders without copying any row data.

### In-Place Cell Modification
Cells can be overwritten in place, as long as the new value serializes to exactly the same number of bytes. This is a single seek and write: no copy of the file and no rewrite of the index. Fixed-width column types (`int*`, `uint*`, `float`, fixed-size `bytes`) are ideal for values you may want to patch later (scores, labels, flags).

```python
with RFDD('data.fdd', allow_cell_modification=True) as rfdd:
    rfdd['key1'].score = 0.93   # attribute assignment writes to disk
```
Only attribute assignment writes to disk; `row['score'] = x` changes the in-memory row only. A size mismatch raises `ValueError`. `WFDD(path, reopen=True, allow_cell_modification=True)` supports the same edits while appending.

### Column Types
| type | encoding | size |
|---|---|---|
| `any` (default) | pickle | variable |
| `str` | UTF-8 | variable |
| `str_compressed` | zlib(UTF-8) | variable |
| `bytes` | raw | variable |
| `int`, `int64` / `int8`, `int16`, `int32`, `int128` | signed little-endian | 8 / 1, 2, 4, 16 bytes |
| `uint`, `uint64` / `uint8`, `uint16`, `uint32`, `uint128` | unsigned little-endian | 8 / 1, 2, 4, 16 bytes |
| `float` | IEEE double | 8 bytes |
| `(serialize_fn, deserialize_fn)` | custom | any |

Missing cells, and values that serialize to zero bytes (e.g. `''` or `b''`), read back as `None`.

### Filtering and Single-Cell Reads
```python
rfdd.filter(lambda row: row.label == 1)        # in-memory subset of the loaded split
RFDD('data.fdd^train$r.label==1')               # same, from a path string
wfdd.make_split('positives', lambda row: row.label == 1)  # persistent split from a predicate
rfdd['key1', 'caption']                         # read one cell without building a row
```
The `$` expression is evaluated with `eval`; never pass untrusted strings.

### Performance Notes
- Opening a keyless split reads a small fixed-size header, so it is effectively instant. Opening a keyed split unpickles its whole index.
- The in-memory index stores packed 6-byte offsets: about `(num_columns + 1) * 6` bytes per row, plus the key objects for keyed splits. Keyless splits stay on disk.
- `make_split(..., preserve_order=False)` stores keys sorted with binary-search lookup, using less memory than the default insertion-ordered index.
- Filtering, `load_keys()`, predicate splits, and `+` unions of keyless splits read every row in the split.

### File Format
Row cells are appended as they are written. On close, the column definitions, custom properties, split indices, and column names are written, followed by a small index of those sections and its 8-byte length. Readers seek to the end of the file and only load what they need.

## Examples

### Example 1: Creating an FDD File
```python
from freeze_dried_data import WFDD

# Create the file "new dataset.fdd"
dataset = WFDD('new dataset.fdd')

# Add the entry 'key1': 'value1' to the dataset
# and write the value to disk
dataset['key1'] = 'value1'

# Add the entry 1234: 5678 to the dataset
# and write the value to disk
dataset[1234] = 5678

# Write the index including all keys to disk and close the file
dataset.close()

# or use a with statement to ensure files are closed when finished
with WFDD('new dataset.fdd', overwrite=True) as dataset:
    dataset['key1'] = 'value1'
    dataset[1234] = 5678
```

### Example 2: Reading an FDD File
```python
from freeze_dried_data import RFDD

# Open the existing file and unpickle the index including all keys into memory
dataset = RFDD('new dataset.fdd')

# Print each key-value pair
for k, v in dataset.items():
  print(k, v)

# Directly access and print specific items
print(dataset[1234])        # prints "5678"
print(list(dataset.keys())) # prints "['key1', 1234]"

dataset.close()

# or using a with statement
with RFDD('new dataset.fdd') as dataset:
    for k, v in dataset.items():
        print(k,v)
    print(dataset[1234])
    print(list(dataset.keys()))
    
```

### Example 3: Creating a file with columns

```python
from freeze_dried_data import WFDD

# Create a new FDD file with columns for 'text' and 'label'
with WFDD('text_dataset.fdd', columns={'text':'str', 'label':'int16'}) as dataset:
    dataset['doc1'] = {'text': 'This is an example document.', 'label': 1}
    dataset['doc2'] = {'text': 'Another document for classification.', 'label': 0}

    # you can also add columns as a tuple
    dataset['doc3'] = ('A third document.', 1)

    # Finally, you can add columns by name
    dataset['doc4'].text = 'A fourth document.'
    dataset['doc4'].label = 0

    # if you don't add all the columns, you can call .finalize() to actually write the data
    dataset['doc5'].text = 'A fifth document.'
    dataset['doc5'].finalize() # doc5 will be written to disk immediately.

    # if you don't call finalize() unfinished rows are kept in memory and written out when the file is closed
    dataset['doc6'].text = 'A sixth document.'

# When the with context exits, the columns, index, and any custom properties are written to disk.

```

### Example 4: Reading a file with columns
```python
from freeze_dried_data import RFDD

with RFDD('text_dataset.fdd') as dataset:
    print(dataset.column_def)
    for key, row in dataset.items():
        print(row.text, row.label)  # equivalently row['text'] or row[0]

# output:
# {'text':'str', 'label':'int16'}
# This is an example document. 1
# Another document for classification. 0
# A third document. 1
# A fourth document. 0
# A fifth document. None
# A sixth document. None

```

### Example 5: Using Custom Properties and Custom Splits
```python
from freeze_dried_data import WFDD, RFDD

# Open a new FDD file, adding custom properties to store additional metadata
with WFDD('dataset_with_properties.fdd') as dataset:
    dataset.creator = 'Data Scientist'
    dataset.creation_date = '2024-04-12'
    dataset.description = 'Sample dataset with custom properties.'

    # Add data to the dataset
    dataset['key1'] = 'train_data1'
    dataset['key2'] = 'train_data2'
    dataset['key3'] = 'train_data3'
    dataset['key4'] = 'val_data1'
    dataset['key5'] = 'val_data2'
    dataset['key6'] = 'test_data1'

    # the training set defined to be an ultra-efficient keyless split
    dataset.make_split('train', ['key1', 'key2', 'key3'], keyless=True)
    dataset.make_split('val', ['key4', 'key5'])
    dataset.make_split('test', ['key6'])


# Verify and print custom properties
with RFDD('dataset_with_properties.fdd', split='train') as loaded_dataset:
    print('Creator:', loaded_dataset.creator)
    print('Creation Date:', loaded_dataset.creation_date)
    print('Description:', loaded_dataset.description)
    # train rows loaded into the index
    loaded_dataset.load_new_split('val')
    # val rows loaded into the index
    loaded_dataset.load_new_split('test')
    # test rows loaded into the index

# Optionally load keys for keyless splits
with RFDD('dataset_with_properties.fdd', split='train') as loaded_dataset:
    loaded_dataset.load_keys(lambda x:x) # keys are now the same as the row.
```
### Example 6: Split operations as part of filename
```python
from freeze_dried_data import WFDD, RFDD
with RFDD('dataset_with_properties.fdd^train') as loaded_dataset:
    pass # training split is loaded

# split types must match to use +; train is keyless here, so recreate it keyed or use val+test
with RFDD('dataset_with_properties.fdd^val+test') as loaded_dataset:
    pass # loads the union of the val and test sets

with RFDD('dataset_with_properties.fdd', split='val+test') as loaded_dataset:
    pass # also loads the union of the val and test sets

with RFDD('dataset_with_properties.fdd') as loaded_dataset:
    pass # loads all rows

with RFDD('dataset_with_properties.fdd^all_rows$r[-1]=="1"') as loaded_dataset:
    pass # loads rows 'train_data1', 'val_data1', 'test_data1' ($ filters require a ^split)

# assumes a second file, dataset_with_properties2.fdd, has been written the same way
with RFDD('dataset_with_properties.fdd,dataset_with_properties2.fdd') as loaded_dataset:
    pass # loads both FDDs as a single FDD object.
```

### Example 7: Using custom Serialization
```python
from freeze_dried_data import WFDD, RFDD

# import inside the functions: dill stores them without their module globals,
# so other processes can load them without this script
def my_serializer(obj):
    import json
    return json.dumps(obj).encode('utf-8')

def my_deserializer(data):
    import json
    return json.loads(data.decode('utf-8'))

# custom serializers and deserializers are saved in the .fdd with dill and loaded in the RFDD
with WFDD('custom_data.fdd', columns={'data': (my_serializer, my_deserializer)}) as dataset:
    dataset['key1'] = {'data': {'complex_data': [1, 2, 3]}}

with RFDD('custom_data.fdd') as dataset:
    print(dataset['key1'].data)

# outputs:
# {'complex_data': [1, 2, 3]}

```

### Example 8: File Reopening
```python
from freeze_dried_data import WFDD

data = {f'key{i}': f'value{i}' for i in range(1000)}
with WFDD('test_file.fdd', overwrite=True) as wfdd:
    for k, v in data.items():
        wfdd[k] = v

    wfdd.custom_attribute1 = 'custom1'
    wfdd.custom_attribute2 = 'custom2'
    wfdd.make_split('evens', [f'key{i}' for i in range(0,1000,2)])

data_2 = {f'key{i}': f'value{i}' for i in range(1000,2000)}
with WFDD('test_file.fdd', reopen=True) as wfdd:
    for k, v in data_2.items():
        wfdd[k] = v
    
    wfdd.custom_attribute1 = 're-written custom1'
    wfdd.custom_attribute3 = 'custom3'

    wfdd.add_to_split('evens', [f'key{i}' for i in range(1000,2000,2)])
    wfdd.make_split('odds', [f'key{i}' for i in range(1,2000,2)])

# test_file.fdd now contains all 2k rows, both full splits, and all three attributes.
```

### Example 9: Using in a PyTorch DataLoader with Workers
```python
from torch.utils.data import Dataset, DataLoader
from freeze_dried_data import WFDD, RFDD

with WFDD('new dataset.fdd', overwrite=True) as dataset:
    dataset['key1'] = 'train_data1'
    dataset['key2'] = 'train_data2'
    dataset['key3'] = 'train_data3'
    dataset['key4'] = 'val_data1'
    dataset['key5'] = 'val_data2'
    # keyless splits are indexed 0..len-1, so no key list is needed in memory
    dataset.make_split('train', ['key1', 'key2', 'key3'], keyless=True)
    dataset.make_split('val', ['key4', 'key5'], keyless=True)

class FDDDataset(Dataset):
    def __init__(self, filename, split='train'):
        self.fdd = RFDD(filename, split=split)
    
    def __len__(self):
        return len(self.fdd)
    
    def __getitem__(self, idx):
        return idx, self.fdd[idx]

dataset = FDDDataset('new dataset.fdd', split='train')

# Each worker gets a separate copy of the dataset object.
# The file handles will be refreshed automatically.
dataloader = DataLoader(dataset, batch_size=2, shuffle=True, num_workers=4)

for key, value in dataloader:
    print(f'Batch: {key} - {value}')

# Example output:
# Batch: tensor([2, 1]) - ('train_data3', 'train_data2')
# Batch: tensor([0]) - ('train_data1',)
```
For keyed splits, keep `self.keys = list(self.fdd.keys())` and look rows up by `self.keys[idx]`.

### Example 10: Modifying an Existing File
```python
from freeze_dried_data import WFDD, RFDD

with WFDD('scores.fdd', columns={'name': 'str', 'score': 'float'}, overwrite=True) as wfdd:
    for i in range(10):
        wfdd[f'key{i}'] = {'name': f'item{i}', 'score': 0.0}
    wfdd.make_split('first_half', [f'key{i}' for i in range(5)])

# overwrite cells in place (same serialized size)
with RFDD('scores.fdd', allow_cell_modification=True) as rfdd:
    rfdd['key3'].score = 0.75

# append rows, edit splits and properties, and hide a row
with WFDD('scores.fdd', reopen=True) as wfdd:
    wfdd['key10'] = {'name': 'item10', 'score': 1.0}
    wfdd.add_to_split('first_half', ['key10'])
    wfdd.make_split('high', lambda row: row.score > 0.5)
    wfdd.version = 2
    wfdd.make_split('all_rows', [k for k in wfdd.keys() if k != 'key9'], overwrite=True)
```
Hiding rows via `all_rows` does not reclaim disk space; rewrite the file to a new FDD for that.

### Adding a Column
`add_column(input_path, output_path, column_name, column_data, column_type='any')` writes a copy with one extra column. It only writes the keys present in `column_data` and copies splits by key, so it does not support keyless splits. For those cases, rewrite the file: assigning an unmodified row read from an RFDD (`wfdd[key] = rfdd[key]`) copies its cells as raw bytes, without deserializing them.
