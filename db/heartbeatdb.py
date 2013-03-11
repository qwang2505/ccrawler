'''
Created on Jul 24, 2012

@author: dhcui
'''

import datetime

from ccrawler.db.utils import configure_db
from ccrawler.db.utils import perf_logging
import ccrawler.common.settings as common_settings
from ccrawler.utils.format import datetime2timestamp, timestamp2datetime
import ccrawler.utils.misc as misc

db = None

_INDEXES = {
}

def config(server="localhost", port=27017, database="heartbeat", dedicated_configs={}):
    global db
    db = configure_db(server, port, database, _INDEXES, dedicated_configs)

if db is None:
    config(common_settings.heart_beat_config.get("database_server", "localhost"),
           common_settings.heart_beat_config.get("database_port", 27017),
           common_settings.heart_beat_config.get("database_name", "heartbeat"),
           common_settings.heart_beat_config.get("dedicated_configs", {}))

@perf_logging
def save_heartbeat(message):
    now = datetime2timestamp(datetime.datetime.utcnow())
    message["_id"] = misc.md5(str(now))
    message["datetime"] = now
    return db.heartbeats.save(message)

@perf_logging
def get_heartbeats(check_duration):
    checkpoint = datetime.datetime.utcnow() - datetime.timedelta(seconds=check_duration)
    return db.heartbeats.find({"datetime" : {"$gt" : datetime2timestamp(checkpoint)}})

@perf_logging
def save_handler_counts(handler_counts, type):
    now = datetime2timestamp(datetime.datetime.utcnow())
    insert = {}
    insert["_id"] = misc.md5(str(now))
    insert["datetime"] = now
    insert["handler_counts"] = handler_counts
    insert["type"] = type
    return db.handlerStatistics.save(insert)
