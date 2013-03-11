
import datetime

from ccrawler.db.utils import configure_db
from ccrawler.utils import misc
from ccrawler.db.utils import perf_logging
import ccrawler.common.settings as common_settings
from ccrawler.utils.format import datetime2timestamp, timestamp2datetime

db = None

_INDEXES = {
}

TOTAL_MINUTE_ROW_COUNT = 60 * 60 * 24 * 14

def config(server="localhost", port=27017, database="diagnostics", dedicated_configs={}):
    global db
    db = configure_db(server, port, database, _INDEXES, dedicated_configs)

if db is None:
    config(common_settings.diagnostics_db_config.get("database_server", "localhost"),
           common_settings.diagnostics_db_config.get("database_port", 27017),
           common_settings.diagnostics_db_config.get("database_name", "diagnostics"),
           common_settings.diagnostics_db_config.get("dedicated_configs", {}))

def get_next_id():
    result = db.counters.find_and_modify({"_id" : "minCounts"}, update = {"$inc" : {"c" : 1}}, upsert = True, new = True)
    count = result["c"] - 1
    if count >= TOTAL_MINUTE_ROW_COUNT:
        count = count % TOTAL_MINUTE_ROW_COUNT
        db.counters.find_and_modify({"_id" : "minCounts", "c" : {"$gt" : TOTAL_MINUTE_ROW_COUNT}}, update = {"$inc" : {"c" : -TOTAL_MINUTE_ROW_COUNT}})
    return count

@perf_logging
def add_inc(crawled_count = 0, modified_count = 0, processed_count = 0):
    inc_map = {"crawled_count" : crawled_count, "modified_count" : modified_count, "processed_count" : processed_count}

    now = datetime.datetime.utcnow()
    checkpoint = datetime.datetime(now.year, now.month, now.day, now.hour, now.minute)
    timestamp = datetime2timestamp(checkpoint)
    update_map = {"exactTime" : now, "checkpoint" : checkpoint}
    update = {"$set" : update_map, "$inc" : inc_map}
    db.minuteCounts.update({"_id" : timestamp}, update, upsert = True)
    db.minuteCounts.remove({"_id" : {"$lt" : datetime2timestamp(checkpoint - datetime.timedelta(seconds = TOTAL_MINUTE_ROW_COUNT))}})

    db.totalCounts.update({"_id" : "totalCounts"}, {"$set" : {"exactTime" : now, "timestamp" : timestamp}, "$inc" : inc_map}, upsert = True)
    db.totalCounts.update({"_id" : "totalCounts", "startTime" : None}, {"$set" : {"startTime" : now}})

@perf_logging
def get_last_minute_count(n):
    return db.minuteCounts.find().sort([("_id", -1)]).limit(n)

@perf_logging
def get_recent_minute_count(n):
    now = datetime2timestamp(datetime.datetime.utcnow()) / 1000
    return db.minuteCounts.find({"_id" : {"$gte" : now - n * 60}}).sort([("_id", -1)])

def _get_recent_time_count(n, hour_or_day):
    now = datetime2timestamp(datetime.datetime.utcnow()) / 1000
    if hour_or_day:
        duration_mins = n * 60
    else:
        duration_mins = n * 60 * 24

    minute_counts = db.minuteCounts.find({"_id" : {"$gte" : now - 60 * duration_mins}})
    time_counts = {}
    for minute_count in minute_counts:
        timestamp = minute_count["_id"]
        time = timestamp2datetime(timestamp)
        if hour_or_day:
            checkpoint = datetime.datetime(time.year, time.month, time.day, time.hour)
        else:
            checkpoint = datetime.datetime(time.year, time.month, time.day)
        timestamp = datetime2timestamp(checkpoint)
        if not time_counts.has_key(timestamp):
            time_counts[timestamp] = {"timestamp" : timestamp, "checkpoint" : checkpoint, "crawled_count" : 0, "modified_count" : 0, "processed_count" : 0}
        time_counts[timestamp]["crawled_count"] += minute_count["crawled_count"]
        time_counts[timestamp]["modified_count"] += minute_count["modified_count"]
        time_counts[timestamp]["processed_count"] += minute_count["processed_count"]
    return sorted(time_counts.values(), key = lambda time_count : time_count["timestamp"])

@perf_logging
def get_recent_hour_count(n):
    return _get_recent_time_count(n, True)

@perf_logging
def get_recent_day_count(n):
    return _get_recent_time_count(n, False)

@perf_logging
def get_total_count():
    return misc.cursor_to_array(db.totalCounts.find())
