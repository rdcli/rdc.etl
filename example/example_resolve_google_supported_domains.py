import socket
import requests
from rdc.etl.harness.threaded import ThreadedHarness
from rdc.etl.hash import Hash
from rdc.etl.io import STDIN
from rdc.etl.transform import Transform
from rdc.etl.transform.extract import Extract
from rdc.etl.transform.filter import Filter
from rdc.etl.transform.join import Join
from rdc.etl.transform.util import Log

url = 'http://www.google.com/supported_domains'

@Extract
def extract_supported_domains():
    r = requests.get(url)
    for tld in r.text.split('\n'):
        yield Hash((('tld', tld), ))

@Filter
def filter_empty(hash, channel=STDIN):
    return len(hash['tld'])

@Transform
def add_www(hash, channel=STDIN):
    return hash.update({'tld': 'www'+hash['tld']})

@Join
def resolve_domain_name(hash, channel=STDIN):
    for family, socktype, proto, canonname, sockaddr in socket.getaddrinfo(hash['tld'], 80):
        yield {
            'family': family,
            'socktype': socktype,
            'proto': proto,
            'canonname': canonname,
            'sockaddr': sockaddr,
            'ip': sockaddr[0],
            }

harness = ThreadedHarness()
harness.add_chain(
    extract_supported_domains,
    filter_empty,
    add_www,
    resolve_domain_name,
    Log()
)
harness()
