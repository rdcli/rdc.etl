from rdc.etl.transform import Transform

class Join(Transform):
    def __init__(self):
        super(Join, self).__init__()

    @abstract
    def get_join_data_for(self, hash):
        pass

    def transform(self, hash):
        for data in self.get_join_data_for(hash):
            yield hash.copy(data)