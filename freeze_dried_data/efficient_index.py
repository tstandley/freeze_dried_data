
from bisect import bisect_left
class FDDIntList:
    def __init__(self, length, buffer, byte_width=6, start_in_buffer=0):
        self.length = length
        self.buffer = buffer
        self.byte_width = byte_width
        self.start_in_buffer = start_in_buffer

    def __getitem__(self,idx):
        if idx < 0:
            idx = self.length + idx
        if idx >= self.length or idx < 0:
            raise IndexError('Index out of bounds', idx, self.length)
        buf = self.buffer[self.start_in_buffer + idx*self.byte_width:self.start_in_buffer + (idx+1)*self.byte_width]
        return int.from_bytes(buf, 'little')
    
    def __len__(self):
        return self.length
        

class FDDIndexBase:
    pass

class FDDIndexKeyless(FDDIndexBase):
    def __init__(self, num_vals, byte_width=6):
        self.buffer=bytearray()
        self.num_vals = num_vals
        self.byte_width = byte_width

    def __getitem__(self, idx):
        if idx >= len(self) or idx < 0:
            raise IndexError('Index out of bounds', idx, len(self))
        return FDDIntList(self.num_vals, self.buffer, byte_width=self.byte_width, start_in_buffer=idx*self.num_vals*self.byte_width)
    
    def keys(self):
        return range(len(self))
    
    def items(self):
        for i in range(len(self)):
            yield i, self[i]

    def values(self):
        for i in range(len(self)):
            yield self[i]

    def __contains__(self, key):
        return key < len(self) and key >= 0
    
    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __len__(self):
        return len(self.buffer) // (self.num_vals * self.byte_width)
    
    def __setitem__(self, idx, val):
        if idx == len(self):
            for i in val:
                self.buffer.extend(i.to_bytes(self.byte_width, 'little'))
        elif idx > len(self):
            raise IndexError('Index out of bounds', idx, len(self))
        else:
            for i, v in enumerate(val):
                self.buffer[idx*self.num_vals*self.byte_width + i*self.byte_width:idx*self.num_vals*self.byte_width + (i+1)*self.byte_width] = v.to_bytes(self.byte_width, 'little')
            

class FDDIndexComparableKey(FDDIndexBase):
    def __init__(self, dict_index, byte_width=6):
        self._keys = list(dict_index.keys())
        self._keys.sort()
        self.num_vals = len(dict_index[self._keys[0]])
        self.buffer=bytearray()
        self.byte_width = byte_width
        for k in self._keys:
            for i in dict_index[k]:
                self.buffer.extend(i.to_bytes(self.byte_width, 'little'))

    def __getitem__(self, key):
        # idx = self.keys.index(key) #make it faster by doing a binary search
        idx = bisect_left(self._keys, key)
        if idx == len(self._keys) or self._keys[idx] != key:
            raise KeyError('Key not found', key)
        
        return FDDIntList(self.num_vals, self.buffer, byte_width=self.byte_width, start_in_buffer=idx*self.num_vals*self.byte_width)

    def keys(self):
        return self._keys

    def items(self):
        for k, v in zip(self._keys, self.values()):
            yield k, v

    def values(self):
        for i in range(len(self._keys)):
            yield FDDIntList(self.num_vals, self.buffer, byte_width=self.byte_width, start_in_buffer=i*self.num_vals*self.byte_width)

    def __contains__(self, key):
        try:
            idx = bisect_left(self._keys, key)
        except TypeError:
            return False
        return idx < len(self._keys) and self._keys[idx] == key    
    
    def __iter__(self):
        for k in self._keys:
            yield self[k]

    def __len__(self):
        return len(self._keys)

class FDDIndexGeneral(FDDIndexBase):
    def __init__(self, num_vals, byte_width=6):
        self.num_vals = num_vals
        #index maps keys to a single int into the array
        self.index = {}
        self.buffer = bytearray()
        self.byte_width = byte_width


    def __getitem__(self, key):
        idx = self.index[key]
        return FDDIntList(self.num_vals, self.buffer, byte_width=self.byte_width, start_in_buffer=idx*self.num_vals*self.byte_width)
    
    def __setitem__(self, key, val):
        
        if len(val) != self.num_vals:
            raise ValueError('Incorrect length of val', len(val), self.num_vals)


        if key not in self.index:
            self.index[key] = len(self.index)
            for i in val:
                self.buffer.extend(i.to_bytes(self.byte_width, 'little'))
        else:
            idx = self.index[key]
            for i, v in enumerate(val):
                self.buffer[idx*self.num_vals*self.byte_width + i*self.byte_width:idx*self.num_vals*self.byte_width + (i+1)*self.byte_width] = v.to_bytes(self.byte_width, 'little')
    
    def __len__(self):
        return len(self.index)
    
    def update(self, dict_index):
        for k in dict_index:
            self[k] = dict_index[k]
    
    def keys(self):
        return self.index.keys()
    
    def items(self):
        for k in self.index:
            yield k, self[k]

    def values(self):
        for k in self.index:
            yield self[k]

    def __contains__(self, key):
        return key in self.index
    
    def __iter__(self):
        for k in self.index:
            yield self[k]



