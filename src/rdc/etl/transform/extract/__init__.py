from rdc.etl.transform import Transform


class Extract(Transform):
    stream_data = []

    def transform(self, h):
        for data in self.stream_data:
            out = h.copy(data)
            yield out