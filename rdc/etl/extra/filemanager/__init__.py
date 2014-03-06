from threading import Thread, Lock, BoundedSemaphore
import requests

class Driver(object):
    pass

class HttpDriver(object):
    max_threads = 4
    max_threads_semaphore = BoundedSemaphore(max_threads)

    @classmethod
    def retrieve(cls, uri):
        with cls.max_threads_semaphore:
            return requests.get(uri)

class FileProxy(object):
    def __init__(self, uri):
        self.uri = uri
        self.lock = Lock()
        self.data = None
        Thread(target=self.__retrieve).start()

    def stat(self):
        with self.lock:
            return self.data, self.data.headers

    def __retrieve(self):
        with self.lock:
            if self.uri.startswith('http://') or self.uri.startswith('https://'):
                self.data = HttpDriver.retrieve(self.uri)
            else:
                raise ValueError('Could not find driver for uri {0}.'.format(repr(self.uri)))

if __name__ == '__main__':
    urls = [
    ]

    files = [FileProxy(url) for url in urls]
    for file in files:
        print file.stat()

