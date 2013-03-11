import httplib
import urllib2
import socket
import ssl

import ccrawler.common.settings as common_settings

"""
#TODO: no need validation, no need reliable_op
"""

def DNSCacheResolver(host):
    ip = common_settings.cache_client().get("dns", host)
    if ip is not None:
        return ip
    else:
        return host

class DNSCacheHTTPConnection(httplib.HTTPConnection):
    def connect(self):
        self.sock = socket.create_connection((DNSCacheResolver(self.host), self.port),self.timeout)

class DNSCacheHTTPSConnection(httplib.HTTPSConnection):
    def connect(self):
        sock = socket.create_connection((DNSCacheResolver(self.host), self.port), self.timeout)
        self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file)

class DNSCacheHTTPHandler(urllib2.HTTPHandler):
    def http_open(self,req):
        return self.do_open(DNSCacheHTTPConnection,req)

class DNSCacheHTTPSHandler(urllib2.HTTPSHandler):
    def https_open(self,req):
        return self.do_open(DNSCacheHTTPSConnection,req)

def enable_dns_cache():
    opener = urllib2.build_opener(DNSCacheHTTPHandler, DNSCacheHTTPSHandler)
    urllib2.install_opener(opener)

def set_dns_cache(host, ip):
    common_settings.cache_client().set("dns", host, data = ip.encode("utf-8", "ignore"))

def has_dns_cache(host):
    return common_settings.cache_client().get("dns", host) is not None
