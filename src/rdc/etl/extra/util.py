# -*- coding: utf-8 -*-
#
# copyright 2012-2014 romain dorgueil
#
# licensed under the apache license, version 2.0 (the "license");
# you may not use this file except in compliance with the license.
# you may obtain a copy of the license at
#
#     http://www.apache.org/licenses/license-2.0
#
# unless required by applicable law or agreed to in writing, software
# distributed under the license is distributed on an "as is" basis,
# without warranties or conditions of any kind, either express or implied.
# see the license for the specific language governing permissions and
# limitations under the license.

from functools import wraps

def TransformBuilder(cls, *args, **kwargs):
    def wrapper(f):
        @wraps(f)
        def wrapped_constructor(*more_args, **more_kwargs):
            kwargs.update(more_kwargs)
            return cls(f, *(args+more_args), **kwargs)
        return wrapped_constructor
    return wrapper


