# Freeze Dried Data
A format for machine learning datasets.

FDD allows your entire dataset to be a single file while supporting fast random access to records on disk.

Work with FDDs like you would python dictionaries. Keys map to objects. For extra organization and speed, columns can be defined such that every row has a value for each column. 

FDD files can operate in either read mode or write mode. Once a file is finalized, it cannot be modified without being re-written (i.e., it is "freeze-dried"). This allows for increased simplicity and speed compared to databases that allow modification.

Values are written to disk immediately upon insertion, while keys are written as part of the index when the FDD file is closed.

## Installation
```bash
pip install freeze-dried-data
```
Alternatively, you can manually move the `freeze_dried_data.py` file into your project directory.

## Features

### Performance Features
- **Selective Data Loading**: The data for a row's column is only loaded from disk when accessed. This allows for dataloaders to ignore certain columns and not incur a performance penalty by loading them from disk.

- **Fast Modified Copying**: When processing a .fdd into a new, modified file, one typically loops through the records in the original file, makes the desired modification/addition to each row, then writes that row out to the new file. FDD directly copies the bits for columns that are unmodified so that no unnecessary serialization/deserialization needs to take place.

- **File Index Is Stored in RAM**: FDD keeps track of the location of each datum in the file using an in-memory index. This allows data to be read in any order with little overhead.

### Custom Splits
Define custom splits in your dataset by storing a list of keys for each split. This allows you to easily manage and access different subsets of your dataset, such as train, val, and test sets.

If you key by hash, storing your train, val, and test sets in the same file can reduce the chance of having train/test or train/val overlap.

Custom Splits can also maintain order. This makes it easy to compare a ciriculum with random training for example.

Splits also take the place of sharding in many cases. With one split per shard, only the index for the selected split is stored in memory. Everything else can remain on disk. And everything can still remain in a single file.

Splits can be loaded as part of the path with the ^ separator. This allows flags for which file to load to also specify which split.

Using the + signifier in the split specificaiton, the union of two or more splits can be loaded (i.e. split='train+val' will load the union of the train and val splits)

### Custom Serialization and Deserialization
FDD allows custom functions for serializing and deserializing data. This flexibility is especially useful when dealing with complex data types or when performance optimizations are necessary. It also allows on-the-fly compression where the compression algorithm can be chosen on a per-column basis. Finally, custom serialization can be much more efficient when storing tensors (numpy, pytorch, etc.)

Custom serializers/deserializers are stored with ```dill``` in the `.fdd` file so that they do not have to be respecified when loaded.

### Custom Properties
Custom properties are a great place to store dataset metadata such as dataset cards or the code/parameters used to generate the data. In read mode, properties are loaded from disk only when accessed, so this need not incur a runtime cost.

### Seamless Integration
FDD is designed to work with data loaders in machine learning frameworks like PyTorch. Unlike other solutions, RFDD objects detect when they've been forked to a new process and re-open their file handles. 

### Context Management
FDD supports Python’s context management (using `with` statements), which ensures that files are properly closed after operations are completed, preventing data corruption and resource leaks.

### Easy Appending to Existing Files
FDD supportes reopening existing files for writing. Reopened files retain all rows, custom properties, and splits already added to the file, while allowing new rows, properties, or splits to be written. Splits can also be modified.

## Examples

### Example 1: Creating an FDD File
```python
from freeze_dried_data import WFDD

# Create the file "new dataset.fdd"
dataset = WFDD('new dataset.fdd')

# Add the entry 'key1': 'value1' to the dataset and write the value to disk
dataset['key1'] = 'value1'

# Add the entry 1234: 5678 to the dataset and write the value to disk
dataset[1234] = 5678

# Write the index including all keys to disk and close the file
dataset.close()

# or use a with statement to ensure files are closed when finished
with WFDD('new dataset.fdd', overwrite=True)
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
with RFDD('new dataset.fdd'):
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
        print(row['text'], row['label'])
        # or
        print(row.text, row.label)
        # or
        print(row[0], row[1])

# output:
# {'text':'str', 'label':'int16'}
# This is an example document. 1
# This is an example document. 1
# This is an example document. 1
# Another document for classification. 0
# Another document for classification. 0
# Another document for classification. 0
# A third document. 1
# A third document. 1
# A third document. 1
# A fourth document. 0
# A fourth document. 0
# A fourth document. 0
# A fifth document. None
# A fifth document. None
# A fifth document. None
# A sixth document. None
# A sixth document. None
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
    dataset.make_split('train', ['key1', 'key2', 'key3'])
    dataset.make_split('val', ['key4', 'key5'])
    dataset.make_split('test', ['key6'])


# Verify and print custom properties
with RFDD('dataset_with_properties.fdd', split='train') as loaded_dataset:
    print('Creator:', loaded_dataset.creator)
    print('Creation Date:', loaded_dataset.creation_date)
    print('Description:', loaded_dataset.description)
    # train rows loaded into the index
    dataset.load_new_split('val')
    # val rows loaded into the index
    dataset.load_new_split('test')
    # test rows loaded into the index

```
### Example 6: Split union and split as part of filename
```python
from freeze_dried_data import WFDD, RFDD
with RFDD('dataset_with_properties.fdd^train') as loaded_dataset:
    pass # loads the training split only

with RFDD('dataset_with_properties.fdd^train+val') as loaded_dataset:
    pass # loads the union of the train and val sets

with RFDD('dataset_with_properties.fdd' split='train+val') as loaded_dataset:
    pass # also loads the union of the train and val sets

with RFDD('dataset_with_properties.fdd') as loaded_dataset:
    pass # loads all rows
```

### Example 7: Using custom Serialization
```python
import json
from freeze_dried_data import WFDD

def my_serializer(obj):
    return json.dumps(obj).encode('utf-8')

def my_deserializer(data):
    return json.loads(data.decode('utf-8'))

# custom serializers and deserializers are saved in the .fdd with dill and loaded in the RFDD
with WFDD('custom_data.fdd', columns={'data': (my_serializer, my_deserializer)}) as dataset:
    dataset['key1'] = {'complex_data': [1, 2, 3]} 

with RFDD('custom_data.fdd') as dataset:
    print(dataset['key1'])

# outputs:
# {'complex_data': [1, 2, 3]}

```

### Example 8: File Reopening
```python
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
import torch
from torch.utils.data import Dataset, DataLoader
from freeze_dried_data import WFDD

with WFDD('new dataset.fdd') as dataset:
    
    dataset['key1'] = 'train_data1'
    dataset['key2'] = 'train_data2'
    dataset['key3'] = 'train_data3'
    dataset['key4'] = 'val_data1'
    dataset['key5'] = 'val_data2'
    dataset.make_split('train', ['key1', 'key2', 'key3'])
    dataset.make_split('val', ['key4', 'key5'])

class FDDDataset(Dataset):
    def __init__(self, filename, split='train'):
        self.fdd = RFDD(filename,split=split)
        self.keys = list(self.fdd.keys())
    
    def __len__(self):
        return len(self.keys)
    
    def __getitem__(self, idx):
        key = self.keys[idx]
        return key, self.fdd[key]

dataset = FDDDataset('new dataset.fdd', split='train')

# Each worker gets a separate copy of the dataset object.
# The file handles will be refreshed automatically.
dataloader = DataLoader(dataset, batch_size=2, shuffle=True, num_workers=4)

for key, value in dataloader:
    print(f'Batch: {key} - {value}')

# Example output:
# Batch: ('key3', 'key2') - ('train_data3', 'train_data2')
# Batch: ('key1',) - ('train_data1',)
```