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

import sys, os, platform
from . import IStatus


def has_ansi_support(handle=None):
    handle = handle or sys.stdout
    if (hasattr(handle, "isatty") and handle.isatty()) or \
            ('TERM' in os.environ and os.environ['TERM'] == 'ANSI'):
        if platform.system() == 'Windows' and not ('TERM' in os.environ and os.environ['TERM'] == 'ANSI'):
            return False
        else:
            return True
    return False


class ConsoleStatus(IStatus):
    def __init__(self):
        self.ansi = has_ansi_support()
        self._lc = 0

    def update(self, transforms):
        # while migrating to new io system, we need to support both list and dict
        if not isinstance(transforms, dict):
            transforms = dict([(id, transforms[id]) for id in range(0, len(transforms))])

        if self.ansi:
            sys.stdout.write("\033[F" * (self._lc))
            print "\033[K", "-" * 80
            for id, transform in transforms.items():
                print "\033[K   ", id, transform
            print "\033[K", "-" * 80
            self._lc = len(transforms) + 2
