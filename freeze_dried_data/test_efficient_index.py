import unittest
from efficient_index import FDDIntList, FDDIndexKeyless, FDDIndexComparableKey, FDDIndexGeneral

class TestFDDIndex(unittest.TestCase):
    def setUp(self):
        self.buffer = bytearray(b'\x01\x00\x00\x00\x00\x02\x00\x00\x00\x00\x03\x00\x00\x00\x00')
        self.fdd_int_list = FDDIntList(3, self.buffer)
        self.fdd_index_keyless = FDDIndexKeyless(3)
        self.fdd_index_comparable_key = FDDIndexComparableKey({10: [1, 2, 3], 20: [4, 5, 6]})
        self.fdd_index_general = FDDIndexGeneral(3)
    
    def test_fdd_int_list_getitem(self):
        self.assertEqual(self.fdd_int_list[0], 1)
        self.assertEqual(self.fdd_int_list[1], 2)
        self.assertEqual(self.fdd_int_list[2], 3)
        with self.assertRaises(IndexError):
            _ = self.fdd_int_list[3]

    def test_fdd_index_keyless_setitem_getitem(self):
        self.fdd_index_keyless[0] = [1, 2, 3]
        self.assertEqual(self.fdd_index_keyless[0][0], 1)
        self.assertEqual(self.fdd_index_keyless[0][1], 2)
        self.assertEqual(self.fdd_index_keyless[0][2], 3)
        with self.assertRaises(IndexError):
            _ = self.fdd_index_keyless[1]

    def test_fdd_index_comparable_key_getitem(self):
        self.assertEqual(self.fdd_index_comparable_key[10][0], 1)
        self.assertEqual(self.fdd_index_comparable_key[10][1], 2)
        self.assertEqual(self.fdd_index_comparable_key[10][2], 3)
        self.assertEqual(self.fdd_index_comparable_key[20][0], 4)
        self.assertEqual(self.fdd_index_comparable_key[20][1], 5)
        self.assertEqual(self.fdd_index_comparable_key[20][2], 6)
        with self.assertRaises(KeyError):
            _ = self.fdd_index_comparable_key[30]

    def test_fdd_index_general_setitem_getitem(self):
        self.fdd_index_general["a"] = [1, 2, 3]
        self.assertEqual(self.fdd_index_general["a"][0], 1)
        self.assertEqual(self.fdd_index_general["a"][1], 2)
        self.assertEqual(self.fdd_index_general["a"][2], 3)
        with self.assertRaises(KeyError):
            _ = self.fdd_index_general["b"]
        with self.assertRaises(ValueError):
            self.fdd_index_general["a"] = [1, 2]  # Incorrect length

if __name__ == "__main__":
    unittest.main()
