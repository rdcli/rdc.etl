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

import time
from threading import Thread
import sys
from rdc.etl.harness import AbstractHarness
from rdc.etl.io import TerminatedInputError, SingleItemQueue


class TransformThread(Thread):
    """Encapsulate a transformation in a thread, handle errors."""

    def __init__(self, transform, group=None, target=None, name=None, args=(), kwargs=None, verbose=None):
        super(TransformThread, self).__init__(group, target, name, args, kwargs, verbose)
        self.transform = transform

    def step(self, finalize=False):
        try:
            self.transform.step(finalize=finalize)
        except TerminatedInputError, e:
            raise
        except Exception, e:
            raise
            self.transform.output.put_error(e)

    def run(self):
        while not self.transform.input.terminated:
            try:
                self.step()
            except TerminatedInputError, e:
                break

        try:
            self.step(finalize=True)
        except TerminatedInputError, e:
            pass

    def __repr__(self):
        return (self.is_alive() and '+' or '-') + ' ' + repr(self.transform)


class IdGenerator(object):
    """Simple integer sequence generator."""

    def __init__(self):
        self.current = 0

    def get(self):
        return self.current

    def next(self):
        self.current += 1
        return self.current


class ThreadedHarness(AbstractHarness):
    """Builder for ETL job python callables, using threads for parallelization."""

    def __init__(self):
        super(ThreadedHarness, self).__init__()
        self._transforms = {}
        self._threads = {}
        self._current_id = IdGenerator()

        # pointer to last added transform, so we can use the chain_add shortcut
        self._last_transform = None

    def validate(self):
        for id, transform in self._transforms.items():
            if not transform.input.plugged:
                transform.input.set_queue(SingleItemQueue())

    def loop(self):
        # todo healthcheck ? (cycles ... dead ends ... orphans ...)

        # start all threads
        for id, thread in self._threads.items():
            thread.start()

        # main loop until all threads are done
        while True:
            is_alive = False
            for id, thread in self._threads.items():
                is_alive = is_alive or thread.is_alive()

            # communicate with the world
            self.update_status()

            # exit point
            if not is_alive:
                break

            # take a nap. Time here determine how often status is updated, and the maximum waste of time after all
            # threads finished.
            time.sleep(0.5)

        # Wait for all transform threads to die
        for id, thread in self._threads.items():
            thread.join()

    def update_status(self):
        for status in self.status:
            status.update(self._transforms)

    # Methods below does not belong to API.
    def add(self, transform):
        id = self._current_id.next()
        self._transforms[id] = transform
        self._threads[id] = TransformThread(transform)
        return transform # BC, maybe id would be a better thing to return (todo 2.0)

    def chain_add(self, *transforms):
        for transform in transforms:
            self.add(transform)

            if self._last_transform:
                transform.input.plug(self._last_transform.output)

            self._last_transform = transform


