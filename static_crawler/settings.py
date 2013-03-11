'''
Created on Sep, 08, 2012

@author: baiwu
'''

import os

from ccrawler.common.settings import log_dir
import ccrawler.static_crawler.configuration as configuration

dns_cache_enabled = True
timeout = 120
default_headers = configuration.default_headers

user_agent_rotation_enabled = False
user_agent_file = os.path.join(os.path.dirname(__file__), ".allagents")
default_user_agent = "Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"

robotstxt_enabled = False

chunked_transfer_decoding = False

# TODO this is not used?
# if want to use async mode, should set async_mode=True
async_downloader_mode = True
downloader_type = "urllib2"

hosted_handlers = {
    "StaticCrawlerHandler" : {"concurrency" : configuration.CONCURRENCY['concurrency']},
}

crawler_db_config = {
    "database_server" : configuration.CRAWLER_DB['server'],
    "database_name" : configuration.CRAWLER_DB['db'],
    "database_port" : configuration.CRAWLER_DB['port'],
}

crawlerMeta_db_config = {
    "database_server" : configuration.CRAWLER_META_DB['server'],
    "database_name" : configuration.CRAWLER_META_DB['db'],
    "database_port" : configuration.CRAWLER_META_DB['port'],
}

transcode_db_config = {
    "database_server" : configuration.TRANSCODE_DB['server'],
    "database_name" : configuration.TRANSCODE_DB['db'],
    "database_port" : configuration.TRANSCODE_DB['port'],
}

mq_client_config = {
    "parameters" : {"host" : configuration.MESSAGE_QUEUE_CONFIG['server'],"port" : configuration.MESSAGE_QUEUE_CONFIG['port'], "virtual_host" : "/","lazy_load" : True},
}

heart_beat_config = {
    "client_enabled" : True,
    "client_interval" : 60 * 5,
    "server_address" : configuration.HEART_BEAT_DB['server_address'],
    "server_port" : 9090,
    "max_data_size" : 1024 * 10,
    "backlog" : 1024,
    "server_interval" : 60 * 5,
    "database_server" : configuration.HEART_BEAT_DB['database_server' ],
    "database_name" : "heartbeat",
    "check_duration" : 60 * 12,
    "notification_duration" : 60 * 30,
    "required_handlers" : ["Processor","RecrawlScheduler","Crawler"],
    "email_server" : "exchdag.baina.com",
    "email_from" : "dhcui@bainainfo.com",
    "email_tos" : ["dhcui@bainainfo.com"],
    "email_title" : "ccrawlerheartbeat failure",
}

LOGGING = {
    'handlers': {
        'static_crawler': {
            'level':'DEBUG',
            'class':'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'verbose',
            'filename': log_dir + '/static_crawler.log',
            'when': 'D',
            'backupCount': 7,
            },

        'static_crawler_err':{
            'level':'WARN',
            'class':'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'verbose',
            'filename': log_dir + '/static_crawler.err',
            'when': 'D',
            'backupCount': 7,
        }
    },
    'loggers' : {
        'default' : {
            "handlers" : ["static_crawler", "static_crawler_err", "debug", "warn", "err"],
            "level" : 'DEBUG',
        }
    },
}
