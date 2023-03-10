import pickle as pkl
import errno
# files are stored as: record,record,record,...,last record,index,index_length

class FDD:
    def __init__(self,filename,write_or_overwrite = False,read_only=False):
        if write_or_overwrite:
            if os.path.exists(filename):
                os.remove(filename)
        if read_only:
            if not os.path.exists(filename):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filename)
        self.filename = filename
        if os.path.exists(filename):
            self.file = open(filename,'rb')
            self.is_open=True
            self.mode='read_mode'
            self.index = self.get_existing_index()
        else:
            self.file = open(filename,'wb')
            self.is_open=True
            self.mode='create_mode'

            self.current_offset=0
            self.index = {}

    def get_existing_index(self):
        self.file.seek(-8,2) #get 8 bytes from end of file.
        index_size = 0
        index_size_data = self.file.read(8)
        index_size = int.from_bytes(index_size_data,'little')
        
        
        self.file.seek(-(8+index_size),2)
        
        index = pkl.loads(self.file.read(index_size))
        return index

    def close(self):
        if self.mode=='create_mode':
            index_data = pkl.dumps(self.index)
            index_data_length = len(index_data)
            
            self.file.write(index_data)
            self.file.write(index_data_length.to_bytes(8,'little'))
            self.file.close()
        else:
            self.file.close()
        self.is_open=False

    def __setitem__(self, key, item):
        if key in self.index:
            raise ValueError("trying to re-insert a record with key,",
                             key,
                             "FDD cannot re assign items.")
        if self.mode=='read_mode':
            raise ValueError("trying to insert into a read-mode FDD. This is not supported.",
                             "key=",key)

        
        pkld = pkl.dumps(item)
        data_len = len(pkld)
        data_range = (self.current_offset,data_len)
        self.current_offset+=data_len
        self.index[key]=data_range
        self.file.write(pkld)

    def __getitem__(self, key):
        start,data_len = self.index[key]
        self.file.seek(start)
        return pkl.loads(self.file.read(data_len))

    def keys(self):
        return self.index.keys()

    def items(self):
        for k in self.keys():
            yield k,self[k]

    def __contains__(self, item):
        return item in self.index

    def __len__(self):
        return len(self.index)
    
    def update(self,dct):
        for k,v in dct.items():
            self[k]=v

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.is_open:
            self.close()
    
    def __del__(self):
        if self.is_open:
            self.close()

def run_unit_tests():
    import os
    if os.path.exists('/tmp/test.fdd'):
        os.remove('/tmp/test.fdd')
    with FDD('/tmp/test.fdd') as check:

        check['happy']='birthday'
        check['good']=['afternoon']
        check[1337]=3.1415
        #check.close()

    with FDD('/tmp/test.fdd') as check2:

        assert(check2['happy']=='birthday')
        assert(check2['good']==['afternoon'])
        assert(check2[1337]==3.1415)
        assert(len(check2)==3)

    print('tests pass')
    os.remove('/tmp/test.fdd')



if __name__=='__main__':
    run_unit_tests()

