import os
import unittest
from freeze_dried_data.freeze_dried_data import FDD
import random



class TestFDDBase(unittest.TestCase):
    test_file = '/tmp/test_fdd.fdd'
    compression_type = 'none'  # This will be set in subclasses

    def setUp(self):
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def tearDown(self):
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_basic_operations(self):
        with FDD(self.test_file, write_or_overwrite=True, compression=self.compression_type) as fdd:
            fdd['hello'] = 'world'
            fdd['number'] = 123

        with FDD(self.test_file, compression=self.compression_type) as fdd:
            self.assertEqual(fdd['hello'], 'world')
            self.assertEqual(fdd['number'], 123)

    def test_multiple_datatypes(self):
        with FDD(self.test_file, write_or_overwrite=True, compression=self.compression_type) as fdd:
            fdd['int'] = 1
            fdd['float'] = 3.14159
            fdd['string'] = 'test'
            fdd['list'] = [1, 2, 3]
            fdd['dict'] = {'key': 'value'}

        with FDD(self.test_file, compression=self.compression_type) as fdd:
            self.assertEqual(fdd['int'], 1)
            self.assertEqual(fdd['float'], 3.14159)
            self.assertEqual(fdd['string'], 'test')
            self.assertEqual(fdd['list'], [1, 2, 3])
            self.assertEqual(fdd['dict'], {'key': 'value'})

    def test_overwrite_existing_file(self):
        # Create a file, then overwrite it and check if old data is cleared
        with FDD(self.test_file, write_or_overwrite=True) as fdd:
            fdd['old'] = 'data'

        with FDD(self.test_file, write_or_overwrite=True) as fdd:
            fdd['new'] = 'data'
            with self.assertRaises(KeyError):
                _ = fdd['old']

    def test_update_operation(self):
        # Test the update method with multiple items
        updates = {'key1': 'value1', 'key2': 'value2'}
        with FDD(self.test_file, write_or_overwrite=True) as fdd:
            fdd.update(updates)

        with FDD(self.test_file) as fdd:
            for key, value in updates.items():
                self.assertEqual(fdd[key], value)

    def test_large_data_handling(self):
        # Test handling of a large number of records
        num_records = 1000  # adjusted to match DataLoader test requirements
        data = {f'key{i}': f'value{i}' for i in range(num_records)}
        with FDD(self.test_file, write_or_overwrite=True) as fdd:
            fdd.update(data)

        with FDD(self.test_file) as fdd:
            self.assertEqual(len(fdd), num_records)
            for key, value in data.items():
                self.assertEqual(fdd[key], value)

    def test_empty_file(self):
        # Test behavior when operating on an empty FDD file
        with FDD(self.test_file, write_or_overwrite=True) as fdd:
            pass

        with FDD(self.test_file) as fdd:
            self.assertEqual(len(fdd), 0)
            with self.assertRaises(KeyError):
                _ = fdd['nonexistent']

    def test_nonexistent_key(self):
        # Test access with a nonexistent key
        with FDD(self.test_file, write_or_overwrite=True) as fdd:
            fdd['exists'] = 'yes'
        
        with FDD(self.test_file) as fdd:
            with self.assertRaises(KeyError):
                _ = fdd['does_not_exist']

    def test_custom_attributes(self):
        # Test setting and getting custom attributes
        with FDD(self.test_file, write_or_overwrite=True) as fdd:
            fdd.property_one = 'initial'
            fdd.property_two = 123
            fdd.property_one = 'changed'
            
            self.assertEqual(fdd.property_one, 'changed')
            self.assertEqual(fdd.property_two, 123)
            # add three records
            fdd['key1'] = 'value1'
            fdd['key2'] = 'value2'
            fdd['key3'] = 'value3'

            self.assertEqual(len(fdd), 3)

        with FDD(self.test_file, read_only=True) as fdd:
            self.assertEqual(fdd.property_one, 'changed')
            self.assertEqual(fdd.property_two, 123)
            dct = {}
            for k,v in fdd.items():
                dct[k] = v
            self.assertEqual(dct, {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'})
    
    def test_delete_custom_attributes(self):
        with FDD(self.test_file, write_or_overwrite=True) as fdd:
            fdd.some_property = 'value'
            self.assertEqual(fdd.some_property, 'value')

            # Now delete the property
            del fdd.some_property

            # Attempting to access it should raise AttributeError
            with self.assertRaises(AttributeError):
                _ = fdd.some_property

        # Re-open to check persistence of deletion
        with FDD(self.test_file, read_only=True) as fdd:
            # The property should not exist anymore, hence an AttributeError should be raised
            with self.assertRaises(AttributeError):
                _ = fdd.some_property

    def test_custom_properties_read_only_mode(self):
        # Test that setting custom properties in read-only mode raises a ValueError
        with FDD(self.test_file, write_or_overwrite=True) as fdd:
            fdd['initial_data'] = 'data'
            fdd.custom_property = 'test_value'  # Set a custom property in write mode

        # Open the file in read-only mode and attempt to set a custom property
        with FDD(self.test_file, read_only=True) as fdd:
            with self.assertRaises(ValueError):
                fdd.custom_property = 'new_value'  # Attempt to modify the property in read-only mode

            # Ensure the original value is still intact
            self.assertEqual(fdd.custom_property, 'test_value')

        # Verify that new properties cannot be added in read-only mode
        with FDD(self.test_file, read_only=True) as fdd:
            with self.assertRaises(ValueError):
                fdd.new_custom_property = 'should_fail'


    def test_dataloader(self):
        from torch.utils.data import DataLoader, Dataset
        # Test DataLoader functionality with multiple workers
        
        # Create a large dataset
        num_records = 100000
        data = {f'key{i}': (f'value{random.random()}', random.random()) for i in range(num_records)}
        with FDD(self.test_file, write_or_overwrite=True) as fdd:
            fdd.update(data)

        class FDDDataset(Dataset):
            def __init__(self, filename):
                self.fdd = FDD(filename, read_only=True)
                self.keys = list(self.fdd.index.keys())
            
            def __len__(self):
                return len(self.fdd)
            
            def __getitem__(self, idx):
                return self.fdd[self.keys[idx]]
        dataset = FDDDataset(self.test_file)
        loader = DataLoader(dataset, batch_size=10, num_workers=8, shuffle=True)
        
        try:
            for i, data in enumerate(loader):
                self.assertIsNotNone(data)
        except Exception as e:
            self.fail(f"DataLoader failed with exception: {e}")

def create_compression_test_class(compression_type):
    # Properly naming the class
    try:
        class_name = f"TestFDDWith{compression_type.capitalize()}Compression"
    except AttributeError:
        class_name = f"TestFDDWithCustomCompression"
    # Creating the class with type()
    return type(class_name, (TestFDDBase,), {'compression_type': compression_type})

# Registering test classes for each compression type
compression_types = ['zlib', 'bz2', 'gzip', 'none', (lambda x: x + b' ', lambda x: x[:-1])]

for comp_type in compression_types:
    # Creating and adding to globals to ensure it's picked up by unittest
    test_class = create_compression_test_class(comp_type)
    globals()[test_class.__name__] = test_class

if __name__ == '__main__':
    unittest.main()
