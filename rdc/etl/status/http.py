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

from webapp2 import RequestHandler
from wsgiref.simple_server import make_server
from threading import Thread
import webapp2
from rdc.etl.status import BaseStatus

class HttpHandler(RequestHandler):
    @property
    def harness(self):
        return self.app.config.get('harness')

    def get(self):
        threads = self.harness._threads.items()

        self.response.write('''<!DOCTYPE html>
<!--[if IE 9]><html class="lt-ie10" lang="en" > <![endif]-->
<html class="no-js" lang="en" >

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>rdc.etl status</title>
    <link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/foundation/5.0.2/css/normalize.min.css">
    <link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/foundation/5.0.2/css/foundation.min.css">
    <script src="//cdnjs.cloudflare.com/ajax/libs/modernizr/2.7.1/modernizr.min.js"></script>
    <style>
        .alive {

        }
        .dead {
            color: grey;
        }
        .thread {
            padding: 0.25rem;
            margin-bottom: 0.25rem;
            background-color: white;
        }

    </style>
</head>

<body>
    <div class="row">
        <div class="right" style="margin-top: 20px">
            <a class="button tiny round" href="/">reload</a>
            <a class="button tiny round alert" href="/halt">halt</a>
        </div>
        <h1>rdc.etl - web status</h1>
    </div>
    <div class="row">
''')

        finished = False
        for id, thread in threads:
            self.response.write('''<div class="panel thread {alive}">
                (<b>{id}</b>) {name} {stats}
            </div>'''.format(
                id=id,
                alive= 'alive' if thread.is_alive() else 'dead',
                name=thread.name,
                stats=thread.transform.get_stats_as_string()
            ))

        self.response.write('''
    </div>
    <script src="//cdnjs.cloudflare.com/ajax/libs/zepto/1.1.1/zepto.min.js"></script>
    <script src="//cdnjs.cloudflare.com/ajax/libs/foundation/5.0.2/js/foundation.min.js"></script>
    <script>$(document).foundation();</script>
</body>
</html>''')

class HttpServerThread(Thread):
    def __init__(self, harness, verbose=None):
        super(HttpServerThread, self).__init__(None, None, None, (), dict(), verbose)
        self.wsgi_app = webapp2.WSGIApplication([
            ('/', HttpHandler),
        ], debug=True, config={
            'harness': harness
        })

    def run(self):
        self._wsgi_server = make_server('0.0.0.0', 5050, app=self.wsgi_app)
        self._wsgi_server.serve_forever()

class HttpStatus(BaseStatus):
    def initialize(self, harness):
        self.server = HttpServerThread(harness)
        self.server.start()

    def update(self, harness):
        pass


