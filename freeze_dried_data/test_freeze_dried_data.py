import os
import unittest
import random
from freeze_dried_data import RFDD, WFDD, add_column, FDDIndexBase

class TestFDD(unittest.TestCase):
    test_file = '/tmp/test_fdd.fdd'
    test_file2 = '/tmp/test_fdd2.fdd'
    test_file3 = '/tmp/test_fdd3.fdd'
    test_file4 = '/tmp/test_fdd4.fdd'

    def setUp(self):
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
        if os.path.exists(self.test_file2):
            os.remove(self.test_file2)
        # if os.path.exists(self.test_file3):
        #     os.remove(self.test_file3)
        # if os.path.exists(self.test_file4):
        #     os.remove(self.test_file4)

    def tearDown(self):
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
        if os.path.exists(self.test_file2):
            os.remove(self.test_file2)
        # if os.path.exists(self.test_file3):
        #     os.remove(self.test_file3)
        # if os.path.exists(self.test_file4):
        #     os.remove(self.test_file4)

    def test_basic_operations_no_columns(self):
        # Write operations
        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['hello'] = 'world'
            wfdd['number'] = 123
            

        # Read operations
        with RFDD(self.test_file) as rfdd:
            self.assertEqual(rfdd['hello'], 'world')
            self.assertEqual(rfdd['number'], 123)

    def test_basic_operations_with_columns(self):
        # Write operations
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'] = {'name': 'house1', 'area': 100, 'price': 100000}
            wfdd['house2'] = {'name': 'house2', 'area': 200, 'price': 200000}
            wfdd['house3'] = ('house3', 300, 300000)

        # Read operations
        with RFDD(self.test_file) as rfdd:
            self.assertTrue('house1' in rfdd)
            self.assertEqual(rfdd['house1'].name, 'house1')
            self.assertEqual(rfdd['house1'].area, 100)
            self.assertEqual(rfdd['house1'].price, 100000)
            self.assertEqual(rfdd['house2'].name, 'house2')
            self.assertEqual(rfdd['house2'].area, 200)
            self.assertEqual(rfdd['house2'].price, 200000)
            self.assertEqual(rfdd['house3'].name, 'house3')
            self.assertEqual(rfdd['house3'].area, 300)
            self.assertEqual(rfdd['house3'].price, 300000)
            self.assertEqual(rfdd['house1']['name'], 'house1')
            self.assertEqual(rfdd['house1']['area'], 100)
            self.assertEqual(rfdd['house1']['price'], 100000)
            self.assertEqual(rfdd['house2']['name'], 'house2')
            self.assertEqual(rfdd['house2']['area'], 200)
            self.assertEqual(rfdd['house2']['price'], 200000)
            self.assertEqual(rfdd['house3']['name'], 'house3')
            self.assertEqual(rfdd['house3']['area'], 300)
            self.assertEqual(rfdd['house3']['price'], 300000)
            self.assertEqual(rfdd['house1'][0], 'house1')
            self.assertEqual(rfdd['house1'][1], 100)
            self.assertEqual(rfdd['house1'][2], 100000)
            self.assertEqual(rfdd['house2'][0], 'house2')
            self.assertEqual(rfdd['house2'][1], 200)
            self.assertEqual(rfdd['house2'][2], 200000)
            self.assertEqual(rfdd['house3'][0], 'house3')
            self.assertEqual(rfdd['house3'][1], 300)
            self.assertEqual(rfdd['house3'][2], 300000)
            self.assertEqual(rfdd['house1','name'], 'house1')
            self.assertEqual(rfdd['house1','area'], 100)
            self.assertEqual(rfdd['house1','price'], 100000)


            for k,v in rfdd.items():
                print(k)
                print(v)

            # prints:

            # name: house1
            # area: 100
            # price: 100000
            #
            # house2
            # name: house2
            # area: 200
            # price: 200000
            #
            # house3
            # name: house3
            # area: 300
            # price: 300000


    def test_file_exists(self):
        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['hello'] = 'world'
            wfdd['number'] = 123
            

        with self.assertRaises(FileExistsError):
            with WFDD(self.test_file, overwrite=False) as wfdd:
                pass

    def test_file_does_not_exist(self):
        with self.assertRaises(FileNotFoundError):
            with RFDD(self.test_file) as rfdd:
                pass

    def test_modify_and_write(self):
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'] = {'name': 'house1', 'area': 100, 'price': 100000}
            wfdd['house2'] = {'name': 'house2', 'area': 200, 'price': 200000}
            wfdd['house3'] = ('house3', 300, 300000)

        with WFDD(self.test_file2, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd,\
             RFDD(self.test_file) as rfdd:
             for k,v in rfdd.items():
                v.price = v.price * 1.5
                wfdd[k+' with inflation'] = v

        with RFDD(self.test_file2) as rfdd:
            self.assertEqual(rfdd['house1 with inflation'].name, 'house1')
            self.assertEqual(rfdd['house1 with inflation'].area, 100)
            self.assertEqual(rfdd['house1 with inflation'].price, 150000)
            self.assertEqual(rfdd['house2 with inflation'].name, 'house2')
            self.assertEqual(rfdd['house2 with inflation'].area, 200)
            self.assertEqual(rfdd['house2 with inflation'].price, 300000)
            self.assertEqual(rfdd['house3 with inflation'].name, 'house3')
            self.assertEqual(rfdd['house3 with inflation'].area, 300)
            self.assertEqual(rfdd['house3 with inflation'].price, 450000)

    def test_modify_and_write_use_indices(self):
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'] = {'name': 'house1', 'area': 100, 'price': 100000}
            wfdd['house2'] = {'name': 'house2', 'area': 200, 'price': 200000}
            wfdd['house3'] = ('house3', 300, 300000)

        with WFDD(self.test_file2, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd,\
             RFDD(self.test_file) as rfdd:
             for k,v in rfdd.items():
                v[2] = v[2] * 1.5
                wfdd[k+' with inflation'] = v

        with RFDD(self.test_file2) as rfdd:
            self.assertEqual(rfdd['house1 with inflation'].name, 'house1')
            self.assertEqual(rfdd['house1 with inflation'].area, 100)
            self.assertEqual(rfdd['house1 with inflation'].price, 150000)
            self.assertEqual(rfdd['house2 with inflation'].name, 'house2')
            self.assertEqual(rfdd['house2 with inflation'].area, 200)
            self.assertEqual(rfdd['house2 with inflation'].price, 300000)
            self.assertEqual(rfdd['house3 with inflation'].name, 'house3')
            self.assertEqual(rfdd['house3 with inflation'].area, 300)
            self.assertEqual(rfdd['house3 with inflation'].price, 450000)

            
    def test_custom_attributes(self):
        # Test setting and getting custom attributes
        with WFDD(self.test_file, overwrite=True, columns={'value':'any'}) as fdd:
            fdd.property_one = 'initial'
            fdd.property_two = 123
            fdd.property_one = 'changed'
            fdd.property_3 = 3.14159

            self.assertEqual(fdd.property_one, 'changed')

            del fdd.property_3
            
            self.assertEqual(fdd.property_one, 'changed')
            self.assertEqual(fdd.property_two, 123)
            # add three records
            fdd['key1'].value = 'value1'
            fdd['key2'].value = 'value2'
            fdd['key3'].value = 'value3'

            self.assertEqual(len(fdd), 3)

        with RFDD(self.test_file) as fdd:
            self.assertEqual(fdd.property_one, 'changed')
            self.assertEqual(fdd.property_one, 'changed') # the second time it should be loaded from the cache
            self.assertEqual(fdd.property_two, 123)
            del fdd.property_one
            with self.assertRaises(AttributeError):
                _ = fdd.property_one
            
            fdd.property_two = 'new'
            self.assertEqual(fdd.property_two, 'new')
            dct = {}
            for k,v in fdd.items():
                dct[k] = v.value
            self.assertEqual(dct, {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'})

    def test_incomplete_columns(self):
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'] = {'name': 'house1', 'area': 100}
            wfdd['house2'] = {'name': 'house2', 'price': 200000}
            wfdd['house3'] = ('house3', 300, 300000)
        
        with RFDD(self.test_file) as rfdd:
            self.assertEqual(rfdd['house1'].name, 'house1')
            self.assertEqual(rfdd['house1'].area, 100)
            self.assertEqual(rfdd['house1'].price, None)
            self.assertEqual(rfdd['house2'].name, 'house2')
            self.assertEqual(rfdd['house2'].area, None)
            self.assertEqual(rfdd['house2'].price, 200000)
            self.assertEqual(rfdd['house3'].name, 'house3')
            self.assertEqual(rfdd['house3'].area, 300)
            self.assertEqual(rfdd['house3'].price, 300000)

    def test_single_column_functionality(self):
        with WFDD(self.test_file, columns={'value':'any'}, overwrite=True) as wfdd:
            wfdd['key1'].value='value1'
            wfdd['key2']['value'] ='value2'
            wfdd['key3'] = ('value3',)

        with RFDD(self.test_file) as rfdd:
            self.assertEqual(rfdd['key1'].value, 'value1')
            self.assertEqual(rfdd['key2'].value, 'value2')
            self.assertEqual(rfdd['key3'].value, 'value3')

            rfdd['key1'].value = 'new_value1'
            self.assertEqual(rfdd['key1'].value, 'new_value1')

            rfdd['key1']['value'] = 'new_value2'
            self.assertEqual(rfdd['key1'].value, 'new_value2')

    def test_set_all_of_column(self):
        with WFDD(self.test_file, columns={'value':'any','area':'any'}, overwrite=True) as wfdd:
            for i in range(100):
                wfdd[f'key{i}'].value = i
                wfdd[f'key{i}'].area = i+1
        
        with RFDD(self.test_file) as rfdd:
            with WFDD(self.test_file2, columns={'value':'any','area':'any'}, overwrite=True) as wfdd2:
                for k,v in rfdd.items():
                    v.value = v.value * 1.1
                    wfdd2[k] = v

        with RFDD(self.test_file2) as rfdd2:
            for i, (k,v) in enumerate(rfdd2.items()):
                self.assertEqual(v.value, i*1.1)
                self.assertEqual(v.area, i+1)



    def test_set_columns_by_attribute_or_item(self):
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'].name = 'house1'
            wfdd['house1'].area = 100
            wfdd['house1'].price = 100000
            wfdd['house2']["name"] = 'house2'
            wfdd['house2']["area"] = 200
            wfdd['house2']["price"] = 200000
            wfdd['house3'] = ('house3', 300, 300000)

        with RFDD(self.test_file) as rfdd:
            self.assertEqual(rfdd['house1'].name, 'house1')
            self.assertEqual(rfdd['house1'].area, 100)
            self.assertEqual(rfdd['house1'].price, 100000)
            self.assertEqual(rfdd['house2'].name, 'house2')
            self.assertEqual(rfdd['house2'].area, 200)
            self.assertEqual(rfdd['house2'].price, 200000)
            self.assertEqual(rfdd['house3'].name, 'house3')
            self.assertEqual(rfdd['house3'].area, 300)
            self.assertEqual(rfdd['house3'].price, 300000)


    def test_out_of_order_writes(self):
        with WFDD(self.test_file, columns={'first':'int', 'second':'int'},overwrite=True) as wfdd:
            for i in range(100):
                wfdd[f'key{i}'].first = i
            for i in range(50):
                wfdd[f'key{i}'].second = i+1

        with RFDD(self.test_file) as rfdd:
            for i in range(100):
                self.assertEqual(rfdd[f'key{i}'].first, i)
                if i < 50:
                    self.assertEqual(rfdd[f'key{i}'].second, i+1)
                else:
                    self.assertEqual(rfdd[f'key{i}'].second, None)
            

    def test_multiple_datatypes(self):
        # Write operations
        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['int'] = 1
            wfdd['float'] = 3.14159
            wfdd['string'] = 'test'
            wfdd['list'] = [1, 2, 3]
            wfdd['dict'] = {'key': 'value'}
            

        # Read operations
        with RFDD(self.test_file) as rfdd:
            self.assertEqual(rfdd['int'], 1)
            self.assertEqual(rfdd['float'], 3.14159)
            self.assertEqual(rfdd['string'], 'test')
            self.assertEqual(rfdd['list'], [1, 2, 3])
            self.assertEqual(rfdd['dict'], {'key': 'value'})

    def test_overwrite_existing_file(self):
        # Create a file and then overwrite it
        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['old'] = 'data'
            

        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['new'] = 'data'
            

        with RFDD(self.test_file) as rfdd:
            with self.assertRaises(KeyError):
                _ = rfdd['old']
            self.assertEqual(rfdd['new'], 'data')


    def test_items_has_len(self):
        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['key1'] = 'value1'
            wfdd['key2'] = 'value2'
            wfdd['key3'] = 'value3'
            
            items = wfdd.items()
            self.assertEqual(len(items), 3)
            keys = wfdd.keys()
            self.assertEqual(len(keys), 3)


    def test_splits(self):

        
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            for i in range(100):
                wfdd[f'house_{i}'] = {'name': f'house_{i}', 'area': 100+10*i, 'price': 1000+100*i}

            wfdd.make_split('odds', [f'house_{i}' for i in range(1,100,2)])
            wfdd.make_split('evens', [f'house_{i}' for i in range(0,100,2)])
            wfdd.make_split('big houses', [f'house_{i}' for i in range(80,100)])
            wfdd.make_split('reverse_order', [f'house_{i}' for i in range(99,-1,-1)])
            with self.assertRaises(ValueError):
                wfdd.make_split('odds', [f'house_{i}' for i in range(1,100,2)])
            with self.assertRaises(ValueError):
                wfdd.make_split('wrong', 'not_a_list')

        with RFDD(self.test_file, split='odds') as rfdd:
            self.assertEqual(len(list(rfdd.keys())), 50)
            self.assertEqual(rfdd.get_available_splits(), ['odds', 'evens', 'big houses', 'reverse_order', 'all_rows'])
            for i, (k,v) in enumerate(rfdd.items()):
                self.assertEqual(k, f'house_{2*i+1}')
                self.assertEqual(v.name, f'house_{2*i+1}')
                self.assertEqual(v.area, 110+20*i)
                self.assertEqual(v.price, 1100+200*i)

            rfdd.load_new_split('evens')
            self.assertEqual(len(list(rfdd.keys())), 50)
            for i, (k,v) in enumerate(rfdd.items()):
                self.assertEqual(k, f'house_{2*i}')
                self.assertEqual(v.name, f'house_{2*i}')
                self.assertEqual(v.area, 100+20*i)
                self.assertEqual(v.price, 1000+200*i)

            rfdd.load_new_split('big houses')
            self.assertEqual(len(list(rfdd.keys())), 20)
            for i, (k,v) in enumerate(rfdd.items()):
                self.assertEqual(k, f'house_{80+i}')
                self.assertEqual(v.name, f'house_{80+i}')
                self.assertEqual(v.area, 900+10*i)
                self.assertEqual(v.price, 9000+100*i)
        with RFDD(f"{self.test_file}^reverse_order") as rfdd:
            self.assertEqual(len(list(rfdd.keys())), 100)
            for i, (k,v) in enumerate(rfdd.items()):
                self.assertEqual(k, f'house_{99-i}')
                self.assertEqual(v.name, f'house_{99-i}')
                self.assertEqual(v.area, 1090-10*i)
                self.assertEqual(v.price, 10900-100*i)

            with self.assertRaises(KeyError):
                rfdd.load_new_split('wrong')

    def test_split_operations(self):

        for index_type in [(False, False), (True, False), (False, True), (True, True)]:
            with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
                for i in range(100):
                    wfdd[f'house_{i}'] = {'name': f'house_{i}', 'area': 100+10*i, 'price': 1000+100*i}

                wfdd.make_split('odds', [f'house_{i}' for i in range(1,100,2)], keyless=index_type[0], preserve_order=index_type[1])
                wfdd.make_split('evens', [f'house_{i}' for i in range(0,100,2)], keyless=index_type[0], preserve_order=index_type[1])
                wfdd.make_split('big houses', [f'house_{i}' for i in range(80,100)], keyless=index_type[0], preserve_order=index_type[1])
                
            with RFDD(self.test_file, split='odds+evens') as rfdd:
                self.assertEqual(len(list(rfdd.keys())), 100)
                for k,v in rfdd.items():
                    i = int(v.name.split('_')[-1])
                    if not index_type[0]:
                        self.assertEqual(k, f'house_{i}')
                    self.assertEqual(v.name, f'house_{i}')
                    self.assertEqual(v.area, 100+10*i)
                    self.assertEqual(v.price, 1000+100*i)

            with RFDD(self.test_file, split='odds+big houses') as rfdd:
                self.assertEqual(len(list(rfdd.keys())), 60)
                for k,v in rfdd.items():
                    i = int(v.name.split('_')[-1])
                    if not index_type[0]:
                        self.assertEqual(k, f'house_{i}')
                    self.assertEqual(v.name, f'house_{i}')
                    self.assertEqual(v.area, 100+10*i)
                    self.assertEqual(v.price, 1000+100*i)

            with RFDD(self.test_file, split='odds+evens+big houses') as rfdd:
                self.assertEqual(len(list(rfdd.keys())), 100)
                for k,v in rfdd.items():
                    i = int(v.name.split('_')[-1])
                    if not index_type[0]:
                        self.assertEqual(k, f'house_{i}')
                    
                    self.assertEqual(v.name, f'house_{i}')
                    self.assertEqual(v.area, 100+10*i)
                    self.assertEqual(v.price, 1000+100*i)

            with RFDD(self.test_file, split='evens+odds') as rfdd:
                self.assertEqual(len(list(rfdd.keys())), 100)
                for k,v in rfdd.items():
                    i = int(v.name.split('_')[-1])
                    if not index_type[0]:
                        self.assertEqual(k, f'house_{i}')
                    self.assertEqual(v.name, f'house_{i}')
                    self.assertEqual(v.area, 100+10*i)
                    self.assertEqual(v.price, 1000+100*i)

            with RFDD(self.test_file, split='big houses+odds') as rfdd:
                self.assertEqual(len(list(rfdd.keys())), 60)
                for k,v in rfdd.items():
                    i = int(v.name.split('_')[-1])
                    if not index_type[0]:
                        self.assertEqual(k, f'house_{i}')
                    self.assertEqual(v.name, f'house_{i}')
                    self.assertEqual(v.area, 100+10*i)
                    self.assertEqual(v.price, 1000+100*i)

            with RFDD(self.test_file, split='big houses+evens+odds') as rfdd:
                self.assertEqual(len(list(rfdd.keys())), 100)
                for k,v in rfdd.items():
                    i = int(v.name.split('_')[-1])
                    if not index_type[0]:
                        self.assertEqual(k, f'house_{i}')
                    
                    self.assertEqual(v.name, f'house_{i}')
                    self.assertEqual(v.area, 100+10*i)
                    self.assertEqual(v.price, 1000+100*i)
                


    def test_large_data_handling(self):
        num_records = 1000
        data = {f'key{i}': f'value{i}' for i in range(num_records)}
        with WFDD(self.test_file, overwrite=True) as wfdd:
            for k, v in data.items():
                wfdd[k] = v
            

        with RFDD(self.test_file) as rfdd:
            self.assertEqual(len(list(rfdd.keys())), num_records)
            for key, value in data.items():
                self.assertEqual(rfdd[key], value)

    def test_reads_in_between_writes(self):
        num_records = 1000
        data = {f'key{i}': f'value{i}' for i in range(num_records)}
        
        with WFDD(self.test_file, overwrite=True) as wfdd:
            for i, (k, v) in enumerate(data.items()):
                wfdd[k] = v
                where_read = random.choice(list(data.keys())[:i+1])
                self.assertEqual(wfdd[where_read], data[where_read])

        with RFDD(self.test_file) as rfdd:
            self.assertEqual(len(list(rfdd.keys())), num_records)
            for key, value in data.items():
                self.assertEqual(rfdd[key], value)

    def test_column_not_found(self):
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'] = {'name': 'house1', 'area': 100, 'price': 100000}
            wfdd['house2'] = {'name': 'house2', 'area': 200, 'price': 200000}
            wfdd['house3'] = ('house3', 300, 300000)
            wfdd['house4'].name = 'house4'
            with self.assertRaises(AttributeError):
                wfdd['house4'].nonexistent = 3

            with self.assertRaises(ValueError):
                wfdd['house5'] = ('house5', 500, 500000, 'extra')

        with RFDD(self.test_file) as rfdd:
            with self.assertRaises(AttributeError):
                _ = rfdd['house1'].nonexistent
            
            with self.assertRaises(AttributeError):
                rfdd['house1'].nonexistent = 3
        
    def test_print_finalize_warning(self):
        with self.assertWarns(UserWarning):
            with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
                for i in range(1100):
                    wfdd[f'house_{i}'].name = f'house_{i}' # we only set the name attribute
            # we should get a warning upon exiting the context

        # rows should be written to disk anyway
        with RFDD(self.test_file) as rfdd:
            for i in range(1100):
                self.assertEqual(rfdd[f'house_{i}'].name, f'house_{i}')
                self.assertEqual(rfdd[f'house_{i}'].area, None)
                self.assertEqual(rfdd[f'house_{i}'].price, None)
        

    def test_key_not_found(self):
        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['key1']= 'value1'
            with self.assertRaises(KeyError):
                _ = wfdd['key2']

    def test_key_already_exists(self):
        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['key1']= 'value1'
            with self.assertRaises(KeyError):
                wfdd['key1'] = 'value2'
            
    def test_invalid_object_for_column_mode(self):
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'] = {'name': 'house1', 'area': 100, 'price': 100000}
            wfdd['house2'] = {'name': 'house2', 'area': 200, 'price': 200000}
            with self.assertRaises(ValueError):
                wfdd['house3'] = 15

        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['house1'] = {'name': 'house1', 'area': 100, 'price': 100000}
            wfdd['house2'] = {'name': 'house2', 'area': 200, 'price': 200000}
            wfdd['house3'] = 15 # this should work when not in column mode
        
    def test_object_set_for_column_mode(self):
        class Test:
            def __init__(self, name, area, price):
                self.name = name
                self.area = area
                self.price = price

        class Test2:
            def __init__(self, time, location):
                self.time = time
                self.location = location


        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'] = Test('house1', 100, 100000)
            wfdd['house2'] = Test('house2', 200, 200000)
            with self.assertRaises(ValueError):
                wfdd['house3'] = Test2('house3', 300)

        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            with self.assertRaises(ValueError):
                wfdd['house1'] = {"happy": "birthday"}
            

        

    def test_reaads_in_between_writes_with_columns(self):
        num_records = 1000
        data = {f'key{i}': {'name': f'name{i}', 'area': random.random(), 'price': random.random()} for i in range(num_records)}
        
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            for i, (k, v) in enumerate(data.items()):
                wfdd[k] = v
                where_read = random.choice(list(data.keys())[:i+1])
                self.assertEqual(wfdd[where_read].name, data[where_read]['name'])
                self.assertEqual(wfdd[where_read].area, data[where_read]['area'])
                self.assertEqual(wfdd[where_read].price, data[where_read]['price'])

        with RFDD(self.test_file) as rfdd:
            self.assertEqual(len(list(rfdd.keys())), num_records)
            for key, value in data.items():
                self.assertEqual(rfdd[key].name, value['name'])
                self.assertEqual(rfdd[key].area, value['area'])
                self.assertEqual(rfdd[key].price, value['price'])

    def test_empty_file(self):
        with WFDD(self.test_file, overwrite=True) as wfdd:
            pass

        with RFDD(self.test_file) as rfdd:
            self.assertEqual(len(list(rfdd.keys())), 0)
            with self.assertRaises(KeyError):
                _ = rfdd['nonexistent']

    def test_single_record(self):
        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['only'] = 'record'
        with RFDD(self.test_file) as rfdd:
            self.assertEqual(rfdd['only'], 'record')
    
    def test_nonexistent_key(self):
        with WFDD(self.test_file, overwrite=True) as wfdd:
            wfdd['exists'] = 'yes'
            
        
        with RFDD(self.test_file) as rfdd:
            with self.assertRaises(KeyError):
                _ = rfdd['does_not_exist']

    def test_custom_serialization_deserialization(self):
        import pickle as pkl
        import zlib
        import bz2
        import gzip
        import torch
        import ctypes

        import json


        def json_serializer(obj):
            if isinstance(obj, FDDIndexBase):
                return pkl.dumps(obj)
            return json.dumps(obj).encode('utf-8')

        def json_deserializer(data):
            try:
                return json.loads(data.decode('utf-8'))
            except:
                return pkl.loads(data)
        
        custom_deserialize_list = [lambda x:pkl.loads(x[:-1]), lambda x: pkl.loads(zlib.decompress(x)), lambda x: pkl.loads(bz2.decompress(x)), lambda x: pkl.loads(gzip.decompress(x)), json_deserializer]
        custom_serialize_list = [lambda x:pkl.dumps(x)+b'\n', lambda x: zlib.compress(pkl.dumps(x)), lambda x: bz2.compress(pkl.dumps(x)), lambda x: gzip.compress(pkl.dumps(x)), json_serializer]


        for custom_serialize, custom_deserialize in zip(custom_serialize_list, custom_deserialize_list):
            with WFDD(self.test_file, overwrite=True, system_serialize=custom_serialize) as wfdd:
                wfdd['key1'] = 'value1'
                wfdd['key2'] = 'value2'
                wfdd['key3'] = 'value3'

            with RFDD(self.test_file, system_deserialize=custom_deserialize) as rfdd:
                self.assertEqual(rfdd['key1'], 'value1')
                self.assertEqual(rfdd['key2'], 'value2')
                self.assertEqual(rfdd['key3'], 'value3')



            
            with WFDD(self.test_file, overwrite=True, system_serialize=custom_serialize) as wfdd:
                wfdd['key1'] = 'value1'
                wfdd['key2'] = 'value2'
                wfdd['key3'] = 'value3'

            with RFDD(self.test_file, system_deserialize=custom_deserialize) as rfdd:
                self.assertEqual(rfdd['key1'], 'value1')
                self.assertEqual(rfdd['key2'], 'value2')
                self.assertEqual(rfdd['key3'], 'value3')

            with WFDD(self.test_file, overwrite=True, columns={'name':'str', 'salary':'int', 'position':(custom_serialize, custom_deserialize)}, system_serialize=custom_serialize) as wfdd:
                wfdd['employee1'] = {'name': 'Alice', 'salary': 100000, 'position': 'manager'}
                wfdd['employee2'] = {'name': 'Bob', 'salary': 80000, 'position': 'engineer'}
                wfdd['employee3'] = {'name': 'Charlie', 'salary': 60000, 'position': 'assistant'}
            
            with RFDD(self.test_file, system_deserialize=custom_deserialize) as rfdd:
                self.assertEqual(rfdd['employee1'].name, 'Alice')
                self.assertEqual(rfdd['employee1'].salary, 100000)
                self.assertEqual(rfdd['employee1'].position, 'manager')
                self.assertEqual(rfdd['employee2'].name, 'Bob')
                self.assertEqual(rfdd['employee2'].salary, 80000)
                self.assertEqual(rfdd['employee2'].position, 'engineer')
                self.assertEqual(rfdd['employee3'].name, 'Charlie')
                self.assertEqual(rfdd['employee3'].salary, 60000)
                self.assertEqual(rfdd['employee3'].position, 'assistant')

        for custom_serialize, custom_deserialize in zip(custom_serialize_list, custom_deserialize_list):
            with WFDD(self.test_file, overwrite=True, columns={'hash':'str', 'tensor':(custom_serialize, custom_deserialize), 'label':'str'}) as wfdd:
                wfdd['hash1'] = {'hash': 'hash1', 'tensor': 1, 'label': 'dog'}
                wfdd['hash2'] = {'hash': 'hash2', 'tensor': 2, 'label': 'cat'}
                wfdd['hash3'] = {'hash': 'hash3', 'tensor': 2, 'label': 'dog'}
                self.assertEqual(wfdd['hash3'].hash, 'hash3')
                self.assertEqual(wfdd['hash3'].tensor, 2)
                self.assertEqual(wfdd['hash3'].label, 'dog')
                self.assertEqual(wfdd['hash1'].hash, 'hash1')
                self.assertEqual(wfdd['hash1'].tensor, 1)
                self.assertEqual(wfdd['hash1'].label, 'dog')
                self.assertEqual(wfdd['hash2'].hash, 'hash2')
                self.assertEqual(wfdd['hash2'].tensor, 2)
                self.assertEqual(wfdd['hash2'].label, 'cat')
                

            with RFDD(self.test_file,) as rfdd:
                for k,v in rfdd.items():
                    self.assertEqual(v.hash, k)
                    self.assertEqual(v.tensor, 1 if k == 'hash1' else 2)
                    self.assertEqual(v.label, 'dog' if k == 'hash1' or k == 'hash3' else 'cat')

        data ={'hash1': {'hash': 'hash1', 'tensor': torch.randn(10,10,dtype=torch.bfloat16), 'label': 'dog'},
               'hash2': {'hash': 'hash2', 'tensor': torch.randn(10,10,dtype=torch.bfloat16), 'label': 'cat'},
               'hash3': {'hash': 'hash3', 'tensor': torch.randn(10,10,dtype=torch.bfloat16), 'label': 'dog'}}
        
        
        def tensor_to_bytes(tensor):
            tensor = tensor.clone()
            if not tensor.is_contiguous():
                tensor = tensor.contiguous()
            ptr = tensor.data_ptr()
            num_bytes = tensor.nelement() * tensor.element_size()
            buffer = (ctypes.c_char * num_bytes).from_address(ptr)
            return bytes(buffer)
        
        
        def bytes_to_tensor(byte_data, shape=(10,10)):
            byte_data = bytearray(byte_data)
            tensor = torch.frombuffer(byte_data, dtype=torch.bfloat16, )
            return tensor.view(shape)
        
        for custom_serialize, custom_deserialize in zip(custom_serialize_list, custom_deserialize_list):
            with WFDD(self.test_file, overwrite=True, columns={'hash':'str', 'tensor':(tensor_to_bytes,bytes_to_tensor), 'label':'any'},system_serialize=custom_serialize, ) as wfdd:
                for k,v in data.items():
                    wfdd[k] = v

            with RFDD(self.test_file, system_deserialize=custom_deserialize) as rfdd:
                for k,v in data.items():
                    self.assertEqual(rfdd[k].hash, v['hash'])
                    self.assertTrue(torch.allclose(rfdd[k].tensor, v['tensor']))
                    self.assertEqual(rfdd[k].label, v['label'])

    def test_row_has_already_been_finalized(self):
        with WFDD(self.test_file,columns={'col1':'any','col2':'any'},overwrite=True) as wfdd:
            wfdd['key1'].col1 = 1
            wfdd['key1'].col2 = 2
            with self.assertRaises(AttributeError):
                wfdd['key1'].col1 = 3

    def test_reopen_file(self):
        data = {f'key{i}': f'value{i}' for i in range(1000)}
        with WFDD(self.test_file, overwrite=True) as wfdd:
            for k, v in data.items():
                wfdd[k] = v

            wfdd.custom_attribute1 = 'custom1'
            wfdd.custom_attribute2 = 'custom2'
            wfdd.make_split('evens', [f'key{i}' for i in range(0,1000,2)])
        with RFDD(self.test_file) as rfdd:
            for k,v in data.items():
                self.assertEqual(rfdd[k], v)
            rfdd.load_new_split('evens')
            self.assertEqual(len(list(rfdd.keys())), 500)
            for i, (k,v) in enumerate(rfdd.items()):
                self.assertEqual(k, f'key{2*i}')
                self.assertEqual(v, f'value{2*i}')

        data_2 = {f'key{i}': f'value{i}' for i in range(1000,2000)}
        with WFDD(self.test_file, reopen=True) as wfdd:

            for k, v in data_2.items():
                wfdd[k] = v
            
            wfdd.custom_attribute1 = 're-written custom1'
            wfdd.custom_attribute3 = 'custom3'
            wfdd.add_to_split('evens', [f'key{i}' for i in range(1000,2000,2)])
            with self.assertRaises(ValueError):
                wfdd.add_to_split('fake_split', ['key1'])
            with self.assertRaises(ValueError):
                wfdd.add_to_split('evens', 234234)
            wfdd.make_split('odds', [f'key{i}' for i in range(1,2000,2)])

        with self.assertRaises(FileNotFoundError):
            WFDD('fake_file_doesnt_exist', reopen=True)

        with RFDD(self.test_file) as rfdd:
            for k,v in data.items():
                self.assertEqual(rfdd[k], v)
            for k,v in data_2.items():
                self.assertEqual(rfdd[k], v)

            rfdd.load_new_split('evens')
            self.assertEqual(len(list(rfdd.keys())), 1000)
            for i, (k,v) in enumerate(rfdd.items()):
                self.assertEqual(k, f'key{2*i}')
                self.assertEqual(v, f'value{2*i}')

            rfdd.load_new_split('odds')
            self.assertEqual(len(list(rfdd.keys())), 1000)
            for i, (k,v) in enumerate(rfdd.items()):
                self.assertEqual(k, f'key{2*i+1}')
                self.assertEqual(v, f'value{2*i+1}')

            self.assertEqual(rfdd.custom_attribute1, 're-written custom1')
            self.assertEqual(rfdd.custom_attribute2, 'custom2')
            self.assertEqual(rfdd.custom_attribute3, 'custom3')


    def test_reopen_file_with_columns(self):
        data = {f'key{i}': {'name': f'name{i}', 'area': random.random(), 'price': random.random()} for i in range(1000)}
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            for k, v in data.items():
                wfdd[k] = v

            wfdd.custom_attribute1 = 'custom1'
            wfdd.custom_attribute2 = 'custom2'
            wfdd.make_split('evens', [f'key{i}' for i in range(0,1000,2)])
        with RFDD(self.test_file) as rfdd:
            for k,v in data.items():
                self.assertEqual(rfdd[k].name, v['name'])
                self.assertEqual(rfdd[k].area, v['area'])
                self.assertEqual(rfdd[k].price, v['price'])
            rfdd.load_new_split('evens')
            self.assertEqual(len(list(rfdd.keys())), 500)
            for i, (k,v) in enumerate(rfdd.items()):
                self.assertEqual(k, f'key{2*i}')
                self.assertEqual(v.name, f'name{2*i}')
                self.assertEqual(v.area, data[f'key{2*i}']['area'])
                self.assertEqual(v.price, data[f'key{2*i}']['price'])

        data_2 = {f'key{i}': {'name': f'name{i}', 'area': random.random(), 'price': random.random()} for i in range(1000,2000)}
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, reopen=True) as wfdd:

            for k, v in data_2.items():
                wfdd[k] = v
            
            wfdd.custom_attribute1 = 're-written custom1'
            wfdd.custom_attribute3 = 'custom3'
            wfdd.add_to_split('evens', [f'key{i}' for i in range(1000,2000,2)])
            with self.assertRaises(ValueError):
                wfdd.add_to_split('fake_split', ['key1'])
            with self.assertRaises(ValueError):
                wfdd.add_to_split('evens', 234234)
            wfdd.make_split('odds', [f'key{i}' for i in range(1,2000,2)])


    def test_columns_with_partial_dicts(self):
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'] = {'name': 'house1', 'area': 100}
            wfdd['house2'] = {'name': 'house2', 'price': 200000}

        with RFDD(self.test_file) as rfdd:
            self.assertEqual(rfdd['house1'].name, 'house1')
            self.assertEqual(rfdd['house1'].area, 100)
            self.assertEqual(rfdd['house1'].price, None)
            self.assertEqual(rfdd['house2'].name, 'house2')
            self.assertEqual(rfdd['house2'].area, None)
            self.assertEqual(rfdd['house2'].price, 200000)

    def test_columns_with_dicts_with_extra_keys(self):
        
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            with self.assertRaises(ValueError):
                wfdd['house1'] = {'name': 'house1', 'area': 100, 'extra': 'extra'}
            with self.assertRaises(ValueError):
                wfdd['house2'] = {'name': 'house2', 'price': 200000, 'extra': 'extra'}

    def test_as_dict(self):
        
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'] = {'name': 'house1', 'area': 100, 'price': 100000}
            wfdd['house2'] = {'name': 'house2', 'area': 200, 'price': 200000}
            wfdd['house3'] = ('house3', 300, 300000)

        with RFDD(self.test_file) as rfdd:
            self.assertEqual(rfdd['house1'].as_dict(), {'name': 'house1', 'area': 100, 'price': 100000})
            self.assertEqual(rfdd['house2'].as_dict(), {'name': 'house2', 'area': 200, 'price': 200000})
            self.assertEqual(rfdd['house3'].as_dict(), {'name': 'house3', 'area': 300, 'price': 300000})

    def test_read_row_features(self):
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            wfdd['house1'] = {'name': 'house1', 'area': 100, 'price': 100000}
            wfdd['house2'] = {'name': 'house2', 'area': 200, 'price': 200000}
            wfdd['house3'] = ('house3', 300, 300000)

        with RFDD(self.test_file) as rfdd:
            self.assertIn('name', rfdd['house1'])
            for k,v in rfdd['house1'].items():
                self.assertIn(k, ['name', 'area', 'price'])
                self.assertIn(v, ['house1', 100, 100000])
                self.assertIn(k, rfdd['house1'])
                
            for k in rfdd['house1']:
                self.assertIn(k, ['name', 'area', 'price'])
            for k in rfdd['house1'].keys():
                self.assertIn(k, ['name', 'area', 'price'])
            for v in rfdd['house1'].values():
                self.assertIn(v, ['house1', 100, 100000])



    def test_alternative_indices(self):
        for index_type in [(False, False), (True, False), (False, True), (True, True)]:
            wfdd_dict = {}
            with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True,) as wfdd:
                for i in range(100):
                    wfdd[f'house_{i}'] = {'name': f'house_{i}', 'area': 100+10*i, 'price': 1000+100*i}
                    wfdd_dict[f'house_{i}'] = {'name': f'house_{i}', 'area': 100+10*i, 'price': 1000+100*i}

                wfdd.make_split('odds', [f'house_{i}' for i in range(1,100,2)], keyless=index_type[0], preserve_order=index_type[1])
                wfdd.make_split('evens', [f'house_{i}' for i in range(0,100,2)], keyless=index_type[0], preserve_order=index_type[1])
                wfdd.make_split('big houses', [f'house_{i}' for i in range(80,100)], keyless=index_type[0], preserve_order=index_type[1])
                wfdd.make_split('reverse_order', [f'house_{i}' for i in range(99,-1,-1)], keyless=index_type[0], preserve_order=index_type[1])
                wfdd.make_split('all_rows', wfdd.keys(), overwrite=True, keyless=index_type[0], preserve_order=index_type[1])
                with self.assertRaises(ValueError):
                    wfdd.make_split('odds', [f'house_{i}' for i in range(1,100,2)], keyless=index_type[0], preserve_order=index_type[1])
                with self.assertRaises(ValueError):
                    wfdd.make_split('wrong', 'not_a_list', keyless=index_type[0], preserve_order=index_type[1])


            with RFDD(self.test_file, split='all_rows') as rfdd:
                self.assertEqual(len(list(rfdd.keys())), 100)
                self.assertEqual(rfdd.get_available_splits(), ['odds', 'evens', 'big houses', 'reverse_order', 'all_rows'])

                for i, (k,v) in enumerate(rfdd.items()):
                    if index_type[0] == False:# keyless
                        self.assertIn(k, wfdd_dict)
                    if index_type[1] == True:# preserve_order
                        
                        self.assertEqual(v.name, f'house_{i}')
                        self.assertEqual(v.area, 100+10*i)
                        self.assertEqual(v.price, 1000+100*i)
                    else:
                        self.assertIn(v.as_dict(), wfdd_dict.values())
                            
                
                rfdd.load_new_split('odds')
                for i, (k,v) in enumerate(rfdd.items()):
                    if index_type[0] == False:# keyless
                        self.assertIn(k, wfdd_dict)
                    if index_type[1] == True:# preserve_order
                        self.assertEqual(v.name, f'house_{i*2+1}')
                        self.assertEqual(v.area, 110+20*i)
                        self.assertEqual(v.price, 1100+200*i)
                    else:
                        self.assertIn(v.as_dict(), wfdd_dict.values())
                        
                    

                rfdd.load_new_split('evens')
                self.assertEqual(len(list(rfdd.keys())), 50)
                for i, (k,v) in enumerate(rfdd.items()):
                    if index_type[0] == False:# keyless
                        self.assertIn(k, wfdd_dict)
                    if index_type[1] == True:# preserve_order
                        self.assertEqual(v.name, f'house_{2*i}')
                        self.assertEqual(v.area, 100+20*i)
                        self.assertEqual(v.price, 1000+200*i)
                    else:
                        self.assertIn(v.as_dict(), wfdd_dict.values())

                        
                    

                rfdd.load_new_split('big houses')
                self.assertEqual(len(list(rfdd.keys())), 20)
                for i, (k,v) in enumerate(rfdd.items()):
                    if index_type[0] == False:# keyless
                        self.assertIn(k, wfdd_dict)
                    if index_type[1] == True:# preserve_order
                        self.assertEqual(v.name, f'house_{80+i}')
                        self.assertEqual(v.area, 900+10*i)
                        self.assertEqual(v.price, 9000+100*i)
                    else:
                        self.assertIn(v.as_dict(), wfdd_dict.values())
                        
                    

                rfdd.load_new_split('reverse_order')
                self.assertEqual(len(list(rfdd.keys())), 100)
                for i, (k,v) in enumerate(rfdd.items()):
                    if index_type[0] == False:# keyless
                        self.assertIn(k, wfdd_dict)
                    if index_type[1] == True:# preserve_order
                        self.assertEqual(v.name, f'house_{99-i}')
                        self.assertEqual(v.area, 1090-10*i)
                        self.assertEqual(v.price, 10900-100*i)
                    else:
                        self.assertIn(v.as_dict(), wfdd_dict.values())
                        
    def test_add_column(self):
        with WFDD(self.test_file, columns={'name':'str','area':'any'}, overwrite=True) as wfdd:
            for i in range(100):
                wfdd[f'house_{i}'] = {'name': f'house_{i}', 'area': 100+10*i}

            wfdd.custom_attribute1 = 'custom1'

            wfdd.make_split('evens', [f'house_{i}' for i in range(0,100,2)])

        new_col = {}
        for i in range(100):
            new_col[f'house_{i}'] = 1000+100*i
        
        
        if os.path.exists(self.test_file3):
            os.remove(self.test_file3)

        add_column(self.test_file, self.test_file3, 'price', new_col)

        with RFDD(self.test_file3) as rfdd:
            for i in range(100):
                self.assertEqual(rfdd[f'house_{i}'].price, 1000+100*i)

            rfdd.load_new_split('evens')
            for i in range(50):
                self.assertEqual(rfdd[f'house_{2*i}'].price, 1000+200*i)

            self.assertEqual(rfdd.custom_attribute1, 'custom1')

        with self.assertRaises(ValueError):
            add_column(self.test_file3, self.test_file4, 'price', new_col)

            

    def test_dataloader_integration(self):
        from torch.utils.data import DataLoader, Dataset

        num_records = 100000
        data = {f'key{i}': (f'value{random.random()}', random.random()) for i in range(num_records)}
        with WFDD(self.test_file, overwrite=True) as wfdd:
            for k, v in data.items():
                wfdd[k] = v

        class FDDDataset(Dataset):
            def __init__(self, filename):
                self.rfdd = RFDD(filename)
                self.keys = list(self.rfdd.keys())
            
            def __len__(self):
                return len(self.rfdd)

            def __getitem__(self, idx):
                key = self.keys[idx]
                return self.rfdd[key]

        dataset = FDDDataset(self.test_file)
        loader = DataLoader(dataset, batch_size=10, num_workers=8, shuffle=True)
        
        for i, data in enumerate(loader):
            self.assertIsNotNone(data)

        dataset.rfdd.close()

    def test_dataloader_integration_with_columns(self):
        from torch.utils.data import DataLoader, Dataset
        import torch

        num_records = 100000
        data = {f'key{i}': {'name': f'name{i}', 'area': random.random(), 'price': random.random()} for i in range(num_records)}
        
        with WFDD(self.test_file, columns={'name':'str','area':'any', 'price':'any'}, overwrite=True) as wfdd:
            for k, v in data.items():
                wfdd[k] = v

        class FDDDataset(Dataset):
            def __init__(self, filename):
                self.rfdd = RFDD(filename)
                self.keys = list(self.rfdd.keys())
            
            def __len__(self):
                return len(self.rfdd)

            def __getitem__(self, idx):
                key = self.keys[idx]
                return self.rfdd[key].area, self.rfdd[key].price
            
        dataset = FDDDataset(self.test_file)
        loader = DataLoader(dataset, batch_size=10, num_workers=8, shuffle=True)

        for i, data in enumerate(loader):
            #assert that we get a list of tensors
            self.assertIsInstance(data, list)
            self.assertIsInstance(data[0], torch.Tensor)
            self.assertIsInstance(data[1], torch.Tensor)
            #assert that the tensors have the correct shape
            self.assertEqual(data[0].shape, (10,))
            self.assertEqual(data[1].shape, (10,))

        dataset.rfdd.close()

            
        
 
if __name__ == '__main__':
    unittest.main()


    

