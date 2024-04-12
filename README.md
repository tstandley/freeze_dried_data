# Freeze Dried Data
A very simple format for machine learning datasets.

FDD treats datasets stored on disk as if they were Python dictionaries. It is designed for speed and simplicity.

Keys can be any picklable objects that are valid dictionary keys, and are stored in memory until the file is closed. Values can be any picklable object and are stored on disk.

FDD files can operate in either read mode or write mode. Once a file is finalized, it cannot be modified (i.e., it is "freeze-dried").

Values are written to disk immediately upon insertion, while keys are written as part of the index when the FDD file is closed.

## Installation
```bash
pip install freeze-dried-data
```
Alternatively, you can manually move the `freeze_dried_data.py` file into your project directory.

## Features

### Custom Properties
- **Custom Properties**: Extend the flexibility of FDD files by allowing users to add custom attributes or metadata to the datasets. These properties are saved and loaded with the file, making it easy to store additional contextual information alongside your data.

### Compression Support
- **Compression**: Optimize storage and speed by compressing the dataset files. FDD supports multiple compression algorithms including zlib, bz2, and gzip, which can significantly reduce the disk space used by large datasets.

### File Modes
- **Explicit File Modes**: Control how files are accessed with `write_or_overwrite` and `read_only` modes. This ensures data integrity by preventing unwanted modifications and handling file access errors gracefully.

### Pythonic Interface
- **Python Dictionary-Like Interface**: FDD files act like Python dictionaries. This makes them intuitive to use for Python developers, as they can employ familiar dictionary operations to interact with the datasets.

### Seamless Integration
- **Seamless Integration with Data Loaders**: FDD is designed to work effortlessly with data loaders in machine learning frameworks like PyTorch. This allows for easy use of FDD files in multi-process data loading, which is essential for efficient training of models on large datasets.

### Context Management
- **Context Management Support**: FDD supports Python’s context management (using `with` statements), which ensures that files are properly closed after operations are completed, preventing data corruption and resource leaks.

## Examples

### Example 1: Creating an FDD File
```python
from freeze_dried_data import FDD

# Create the file "new dataset.fdd" if it does not already exist
dataset = FDD('new dataset.fdd')

# Add the entry 'key1': 'value1' to the dataset and write the value to disk
dataset['key1'] = 'value1'

# Add the entry 1234: 5678 to the dataset and write the value to disk
dataset[1234] = 5678

# Write the index including all keys to disk and close the file
dataset.close()
```

### Example 2: Reading an FDD File
```python
from freeze_dried_data import FDD

# Open the existing file and unpickle the index including all keys into memory
dataset = FDD('new dataset.fdd')

# Print each key-value pair
for k, v in dataset.items():
  print(k, v)

# Directly access and print specific items
print(dataset[1234])        # prints "5678"
print(list(dataset.keys())) # prints "['key1', 1234]"
```

### Example 3: Using Explicit File Modes
```python
from freeze_dried_data import FDD

# Create a new dataset or overwrite an existing one
with FDD('modifiable dataset.fdd', write_or_overwrite=True) as mod_dataset:
    mod_dataset['new_key'] = 'new_value'

# Attempt to open an existing file for reading; throw error if the file does not exist
try:
    with FDD('modifiable dataset.fdd', read_only=True) as existing_dataset:
        print(existing_dataset['new_key'])  # prints 'new_value'
except FileNotFoundError:
    print("Dataset does not exist.")
```

### Example 4: Using Compression
```python
from freeze_dried_data import FDD

# Open a new FDD file with zlib compression
with FDD('compressed dataset.zlib.fdd', write_or_overwrite=True, compression='zlib') as compressed_dataset:
    compressed_dataset['compressed_key'] = 'compressed_value'

# Automatically close and write out the file at the end of the 'with' block

# Reading compressed data
with FDD('compressed dataset.zlib.fdd', read_only=True, compression='zlib') as compressed_dataset:
    print(compressed_dataset['compressed_key'])  # prints 'compressed_value'
```

### Example 5: Using Custom Properties
```python
from freeze_dried_data import FDD

# Open a new FDD file, adding custom properties to store additional metadata
with FDD('dataset_with_properties.fdd', write_or_overwrite=True) as dataset:
    dataset.creator = 'Data Scientist'
    dataset.creation_date = '2024-04-12'
    dataset.description = 'Sample dataset with custom properties.'

    # Add data to the dataset
    dataset['key1'] = 'value1'

# Verify and print custom properties
with FDD('dataset_with_properties.fdd', read_only=True) as loaded_dataset:
    print('Creator:', loaded_dataset.creator)
    print('Creation Date:', loaded_dataset.creation_date)
    print('Description:', loaded_dataset.description)
```

### Example 6: Using in a PyTorch DataLoader with Workers
```python
import torch
from torch.utils.data import Dataset, DataLoader
from freeze_dried_data import FDD

class FDDDataset(Dataset):
    def __init__(self, filename):
        self.fdd = FDD(filename, read_only=True)
        self.keys = list(self.fdd.index.keys())
    
    def __len__(self):
        return len(self.fdd)
    
    def __getitem__(self, idx):
        key = self.keys[idx]
        return key, self.fdd[key]

dataset = FDDDataset('new dataset.fdd')
dataloader = DataLoader(dataset, batch_size=2, shuffle=True, num_workers=4)

for key, value in dataloader:
    print(f'Batch: {key} - {value}')
```
