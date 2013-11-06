# -*- coding: utf-8 -*-
#
# Copyright 2012-2013 Romain Dorgueil
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from rdc.etl.hash import Hash
from rdc.etl.io import STDIN
from rdc.etl.transform import Transform


class Join(Transform):
    """
    Join some key => value pairs, that can depend on the source hash.

    This element can change the stream length, either positively (joining >1 item data) or negatively (joining <1 item data)

    """

    is_outer = False

    """
    Return default join data when an outer join is requested but join data is empty. Not used in the default inner
    join case, because no row will be returned if current row did not generate join data.

    """
    default_outer_join_data = Hash()

    def __init__(self, is_outer=False, default_outer_join_data=None):
        super(Join, self).__init__()
        self.is_outer = is_outer or self.is_outer
        self.default_outer_join_data = default_outer_join_data or self.default_outer_join_data

    @abstract
    def get_join_data_for(self, hash):
        """
        Abtract method that must be implemented in concrete subclasses, to return the data that should be joined with
        the given row.

        It should be iterable, or equivalent to False in a test.

        If the result is iterable and its length is superior to 0, the result of this transform will be a cartesian
        product between this method result and the original input row.

        If the result is false or iterable but 0-length, the result of this transform will depend on the join type,
        determined by the is_outer attribute.

        - If is_outer == True, the transform output will be a simple union between the input row and the result of
          self.get_default_outer_join_data()
        - If is_outer == False, this row will be sinked, and will not generate any output from this transform.

        Default join type is inner, to preserve backward compatibility.

        """
        pass

    def transform(self, hash, channel=STDIN):
        join_data = self.get_join_data_for(hash)

        cnt = 0
        if join_data:
            for data in join_data:
                yield hash.copy(data)
                cnt += 1

        if not cnt and self.is_outer:
            yield hash.copy(self.default_outer_join_data)

