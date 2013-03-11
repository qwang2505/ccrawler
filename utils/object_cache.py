import datetime

from ccrawler.utils.format import datetime2timestamp

global_caches = {}

def get(cache_type, cache_key):
    if not global_caches.has_key(cache_type):
        return None
    elif not global_caches[cache_type].has_key(cache_key):
        return None
    else:
        item = global_caches[cache_type][cache_key]
        return item["data"]

def set(cache_type, cache_key, data, expiry=None, warn_count=500, max_count=1000):
    if not global_caches.has_key(cache_type):
        global_caches[cache_type] = {}

    current_cache = global_caches[cache_type]
    now = datetime.datetime.utcnow()
    timestamp = datetime2timestamp(now)
    if expiry is not None:
        expiry = datetime2timestamp(now + datetime.timedelta(seconds = expiry))

    #removes expired caches
    if len(current_cache) >= warn_count:
        for cache_key, item in current_cache.items():
            if item["expiry"] is not None and item["expiry"] <= timestamp:
                current_cache.pop(cache_key)

    #removes oldest item if exceeded
    if not current_cache.has_key(cache_key) and len(current_cache) >= max_count:
        oldest_pair = min(current_cache.items(), key = lambda pair : pair[1]["timestamp"])
        current_cache.pop(oldest_pair[0])

    current_cache[cache_key] = {"data" : data, "timestamp" : timestamp, "expiry" : expiry}
