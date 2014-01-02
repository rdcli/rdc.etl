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

import os
import datetime
import psutil
from repoze.lru import lru_cache
from rdc.etl.status import BaseStatus
from rdc.etl.util import terminal as t


@lru_cache(1, timeout=1)
def memory_usage():
    process = psutil.Process(os.getpid())
    return process.get_memory_info()[0] / float(2 ** 20)


@lru_cache(64, timeout=1)
def execution_time(harness):
    return datetime.datetime.now() - harness._started_at


class ConsoleStatus(BaseStatus):
    """
    Outputs status information to the connected stdout. Can be a TTY, with or without support for colors/cursor
    movements, or a non tty (pipe, file, ...). The features are adapted to terminal capabilities.

    .. attribute:: prefix

        String prefix of output lines.

    """

    def __init__(self, prefix=''):
        self.prefix = prefix

    def _write(self, harness, debug, profile, rewind):
        threads = harness.get_threads()
        if profile:
            append = (
                (u'Memory', '{0:.2f} Mb'.format(memory_usage())),
                (u'Total time', '{0} s'.format(execution_time(harness))),
            )
        else:
            append = ()
        self.write(threads, prefix=self.prefix, append=append, debug=debug, profile=profile, rewind=rewind)

    def update(self, harness, debug, profile):
        if t.is_a_tty:
            self._write(harness, debug, profile, rewind=True)

    def finalize(self, harness, debug, profile):
        self._write(harness, debug, profile, rewind=False)

    @staticmethod
    def write(threads, prefix='', rewind=True, append=None, debug=False, profile=False):
        t_cnt = len(threads)

        for id, thread in threads:
            if thread.is_alive():
                _line = ''.join((t.black('({})'.format(id)), ' ', t.bold(t.white('+')), ' ', thread.name, ' ',
                                 thread.transform.get_unicode_stats(debug=debug, profile=profile), ' ', ))
            else:
                _line = t.black(''.join(
                    ('({})'.format(id), ' - ', thread.name, ' ', thread.transform.get_unicode_stats(debug=debug, profile=profile), ' ', )))
            print prefix + _line + t.clear_eol

        if append:
            # todo handle multiline
            print ' `->', ' '.join(u'{0}: {1}'.format(t.bold(t.white(k)), v) for k, v in append), t.clear_eol
            t_cnt += 1

        if rewind:
            print t.clear_eol
            print t.move_up * (t_cnt + 2)


