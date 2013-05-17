from rdc.etl.transform import Transform


class Extract(Transform):
    stream_data = []

    def transform(self, hash):
        for data in self.stream_data:
            out = hash.copy(data)
            yield out