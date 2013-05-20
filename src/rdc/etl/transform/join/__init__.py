from rdc.etl.transform import Transform

class Join(Transform):
    """
    Join some key => value pairs, that can depend on the source hash.

    This element can change the stream length, either positively (joining >1 item data) or negatively (joining <1 item data)

    """

    def __init__(self):
        super(Join, self).__init__()

    @abstract
    def get_join_data_for(self, hash):
        pass

    def transform(self, h):
        join_data = self.get_join_data_for(h)

        if join_data is not None:
            for data in join_data:
                yield h.copy(data)