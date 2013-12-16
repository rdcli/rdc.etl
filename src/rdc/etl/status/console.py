# -*- coding: utf-8 -*-
#
# Copyright 2012-2014 Romain Dorgueil
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
from rdc.etl.status import IStatus
from rdc.etl.util import terminal as t


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
    """
    Outputs status information to the connected stdout. Can be a TTY, with or without support for colors/cursor
    movements, or a non tty (pipe, file, ...). The features are adapted to terminal capabilities.

    .. attribute:: prefix

        String prefix of output lines.

    """
    def __init__(self, prefix=''):
        self.ansi = has_ansi_support()
        self.prefix = prefix

    def initialize(self, harness):
        pass

    def update(self, harness):
        threads = harness._threads.items()
        if t.is_a_tty:
            self.write(threads, prefix=self.prefix)

    def finalize(self, harness):
        pass

    @staticmethod
    def write(threads, prefix='', rewind=True):
        t_cnt = len(threads)

        for id, thread in threads:
            if thread.is_alive():
                _line = ''.join((t.black('({})'.format(id)), ' ', t.bold(t.white('+')), ' ',  thread.name, ' ', thread.transform.get_stats_as_string(), ' ', ))
            else:
                _line = t.black(''.join(('({})'.format(id), ' - ', thread.name, ' ', thread.transform.get_stats_as_string(), ' ', )))
            print prefix + _line + t.clear_eol
        if rewind:
            print t.clear_eol
            print t.move_up * (t_cnt + 2)


