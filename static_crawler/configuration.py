'''
Created on Sep, 08, 2012

@author: dhcui
'''
from ccrawler.utils.format import ReadOnlyPropertyTable

CRAWLER_DB = ReadOnlyPropertyTable({
    'server' : 'localhost',
    'port' : 27017,
    'db' : 'crawler'
})

CRAWLER_META_DB = ReadOnlyPropertyTable({
    'server' : 'localhost',
    'port' : 27017,
    'db' : 'crawlerMeta'
})

TRANSCODE_DB = ReadOnlyPropertyTable({
    'server' : 'localhost',
    'port' : 27017,
    'db' : 'ccrawler'
})

LOG_DIR = "/var/app/ccrawler/log/"
DATA_DIR = "/var/app/ccrawler/data/"
MESSAGE_QUEUE_CONFIG = ReadOnlyPropertyTable({
    'server' : 'localhost',
    'port' : 5672,
})

HEART_BEAT_DB = ReadOnlyPropertyTable({
    "server_address" : 'localhost',
    'database_server' : 'localhost'
})

CONCURRENCY = ReadOnlyPropertyTable({
    "concurrency" : '10',
})

default_headers = {
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'zh',
}
