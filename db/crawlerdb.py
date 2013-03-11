import datetime

import pymongo.errors

from ccrawler.db.utils import configure_db
from ccrawler.db.utils import perf_logging, default_cond, make_fields
import ccrawler.utils.misc as misc
from ccrawler.utils.format import datetime2timestamp
import ccrawler.common.settings as common_settings
import ccrawler.db.crawlerMetadb as crawlerMetadb
from ccrawler.cache.url_cache_client import UrlCacheClient

db = None

_INDEXES = {
    'urlRepository' : [
        "recrawl_time",
    ],
    'crawlDomainWhitelist' : [
        'domain_type',
        [('domain_type', 1), ('domain', 1)]
    ],
}

def config(server="localhost", port=27017, database="crawler", dedicated_configs={}):
    global db
    db = configure_db(server, port, database, _INDEXES, dedicated_configs)

if db is None:
    config(common_settings.crawler_db_config.get("database_server", "localhost"),
           common_settings.crawler_db_config.get("database_port", 27017),
           common_settings.crawler_db_config.get("database_name", "crawler"),
           common_settings.crawler_db_config.get("dedicated_configs", {}))


def _cond_get_url_info(cond, fields):
    fields = make_fields(fields)
    return db.urlRepository.find_one(cond, fields = fields)

@perf_logging
def get_url_info(url, fields):
    """
    Enabled cache
    """
    url_info = UrlCacheClient.get_url_info(url, fields)
    if url_info is not None:
        return url_info
    else:
        return _cond_get_url_info(default_cond(url), fields)

@perf_logging
def get_url_info_by_status(url, crawl_status, fields):
    """
    Enabled cache
    """
    url_info = UrlCacheClient.get_url_info_by_status(url, crawl_status, fields)
    if url_info is None:
        cond = default_cond(url)
        cond["crawl_status"] = crawl_status
        return _cond_get_url_info(cond, fields)
    elif url_info == False:
        return None
    else:
        return url_info

@perf_logging
def find_and_modify_expired_url_info(expired_time, fields):
    """
    Disabled cache
    """

    cond = {"recrawl_time" : {"$lte" : datetime2timestamp(expired_time)}, "crawl_status" : "alive"}
    now = datetime2timestamp(datetime.datetime.utcnow())
    update_map = {"crawl_status" : "crawling", "last_crawl_start_time" : now}
    return _cond_update_url_info(cond, update_map, None, fields)

def assign_url_info_defaults(url, url_info):
    url_info["_id"] = misc.md5(url)
    now = datetime2timestamp(datetime.datetime.utcnow())
    url_info["created_time"] = now
    url_info["crawled_count"] = 0
    url_info["url_class"] = None
    url_info["error_messages"] = []
    #url_info["processed_count"] = 0
    #url_info["last_processed"] = None
    url_info["first_modified"] = None
    url_info["last_modified"] = None
    url_info["modified_count"] = 0
    url_info["valid_link_count"] = None
    url_info["retry_count"] = 0
    url_info["status_last_modified"] = now
    url_info["encoding"] = None
    url_info["encoding_created_time"] = None
    url_info["redirect_url"] = None
    #url_info["last_finished"] = None
    #url_info["expires"] = now
    url_info["doc"] = None
    url_info["headers"] = None
    url_info["md5"] = None
    #url_info["process_status"] = True
    url_info["last_discovered"] = now
    url_info["discovered_count"] = 1
    url_info["comments"] = ""
    url_info["redirect_count"] = 0
    url_info["recrawl_time"] = now
    url_info["recrawl_duration"] = 0
    url_info["recrawl_priority"] = url_info["crawl_priority"]

    _, full_domain, _ = misc.get_url_domain_info(url)
    url_info["full_domain"] = full_domain

def _insert_url_info(url, url_info):
    UrlCacheClient.update_url_info(url, url_info)

    first_update_map, second_update_map = misc.separate_dict(url_info, common_settings.database_table_fields["urlRepositoryMeta"])
    misc.copy_dict(first_update_map, second_update_map, common_settings.common_url_info_fields + ["url", "_id"])

    db.urlRepository.insert(first_update_map)
    crawlerMetadb.insert_url_info_meta(second_update_map)

def _merge_url_info(url, existing_url_info, url_info, update_map):
    priority_promoted = False
    #Note: consistency issue: may be there is concurrent update here, and make higher value to be changed to lower value.
    if existing_url_info["crawl_priority"] > url_info["crawl_priority"]:
        update_map["crawl_priority"] = url_info["crawl_priority"]
        priority_promoted = True
    else:
        url_info["crawl_priority"] = existing_url_info["crawl_priority"]

    if existing_url_info["crawl_depth"] < url_info["crawl_depth"]:
        update_map["crawl_depth"] = url_info["crawl_depth"]
    else:
        url_info["crawl_depth"] = existing_url_info["crawl_depth"]

    return priority_promoted

@perf_logging
def add_url_info(url, url_info, merge = False):
    """
    Enabled cache
    """

    assign_url_info_defaults(url, url_info)

    existed, alive = UrlCacheClient.check_url_exists(url)
    if not existed:
        _insert_url_info(url, url_info)
        return True, False
    elif alive and merge:
        now = datetime2timestamp(datetime.datetime.utcnow())
        update_map = {"last_discovered" : now}

        # duplicate crawl request merge, will promote crawl_priority/crawl_depth if any
        fields = ["crawl_priority", "crawl_depth", "crawl_status", "url_class", "last_crawled"]
        existing_url_info = get_url_info(url, fields = fields)
        if existing_url_info is None:
            return False, False
        priority_promoted = _merge_url_info(url, existing_url_info, url_info, update_map)

        promoted = False
        misc.copy_dict(existing_url_info, url_info, fields = ["crawl_status", "url_class", "last_crawled"])
        if common_settings.core_settings["general_crawl_policies"]["preemptive_priority_promotion"] and url_info["last_crawled"] is None and priority_promoted:
            if url_info["crawl_status"] == "crawling":
                update_map["expires"] = now
                promoted = True

        update_url_info(url, update_map, {"discovered_count" : 1})
        return False, promoted
    else:
        return False, False

def _make_update(update_map, inc_map = None):
    now = datetime2timestamp(datetime.datetime.utcnow())

    #add status_last_modified field
    if update_map.has_key("crawl_status"):
        update_map["status_last_modified"] = now

    #separate url_info fields from meta_url_info fields
    first_update_map, second_update_map = misc.separate_dict(update_map, common_settings.database_table_fields["urlRepositoryMeta"])
    first_inc_map, second_inc_map = misc.separate_dict(inc_map if inc_map is not None else {}, common_settings.database_table_fields["urlRepositoryMeta"])
    misc.copy_dict(first_update_map, second_update_map, common_settings.common_url_info_fields, soft = True)
    misc.copy_dict(first_inc_map, second_inc_map, common_settings.common_url_info_fields, soft = True)

    first_update = _create_update(first_update_map, first_inc_map)
    second_update = _create_update(second_update_map, second_inc_map)
    return first_update, second_update

def _create_update(update_map, inc_map):
    if len(update_map) == 0 and len(inc_map) == 0:
        return None

    now = datetime2timestamp(datetime.datetime.utcnow())
    update = {"$set" : update_map}
    if inc_map is not None:
        update["$inc"] = inc_map

    #adjust error_message field
    if update_map.has_key("error_message"):
        if not update_map.has_key("error_type"):
            raise Exception("error_type is required if error_message is set")
    if update_map.has_key("error_type"):
        if not update_map.has_key("error_message"):
            raise Exception("error_message is required if error_type is set")

    if update_map.has_key("error_message"):
        error_message = update_map["error_message"]
        error_type = update_map["error_type"]
        error_message = {"timestamp": now, "message" : error_message, "type" : error_type}
        update["$push"] = {"error_messages" : error_message}
        update_map.pop("error_message")
        update_map.pop("error_type")
    return update

def _async_update_url_info(cond, update_map, inc_map):
    first_update, second_update = _make_update(update_map, inc_map)
    if first_update is not None:
        db.urlRepository.update(cond, first_update)
    if second_update is not None:
        crawlerMetadb.update_url_info_meta(cond, second_update)

@perf_logging
def update_url_info(url, update_map, inc_map = None):
    """
    Enabled cache
    """

    UrlCacheClient.update_url_info(url, update_map, inc_map)
    cond = default_cond(url)
    _async_update_url_info(cond, update_map, inc_map)

def _cond_update_url_info(cond, update_map, inc_map = None, fields = ["_id"]):
    '''
    Notes: fields are just fields from urlRepository, while update_map/inc_map can include metaUrlRepository fields.
    updates for metaUrlRepository fields just support async mode.
    '''

    fields = make_fields(fields)
    first_update, second_update = _make_update(update_map, inc_map)

    if second_update is not None:
        crawlerMetadb.update_url_info_meta(cond, second_update)

    if first_update is not None:
        return db.urlRepository.find_and_modify(cond, first_update, fields=fields)
    else:
        return _cond_get_url_info(cond, fields)

@perf_logging
def find_and_modify_url_info(url, update_map, inc_map, fields):
    """
    Enabled cache
    """

    cond = default_cond(url)
    url_info = UrlCacheClient.find_and_modify_url_info(url, update_map, inc_map, fields)
    if url_info is None:
        return _cond_update_url_info(cond, update_map, inc_map, fields)
    else:
        _async_update_url_info(cond, update_map, inc_map)
        return url_info

@perf_logging
def find_and_modify_url_info_by_status(url, crawl_status, update_map, inc_map, fields):
    """
    Enabled cache
    """

    cond = default_cond(url)
    cond["crawl_status"] = crawl_status
    url_info = UrlCacheClient.find_and_modify_url_info_by_status(url, crawl_status, update_map, inc_map, fields)
    if url_info is None:
        return _cond_update_url_info(cond, update_map, inc_map, fields)
    elif url_info == False:
        return None
    else:
        _async_update_url_info(cond, update_map, inc_map)
        return url_info

@perf_logging
def update_url_info_by_status(url, crawl_status, update_map, inc_map = None):
    """
    Enabled cache
    """

    success = UrlCacheClient.update_url_info_by_status(url, crawl_status, update_map, inc_map)
    cond = default_cond(url)
    cond["crawl_status"] = crawl_status
    _async_update_url_info(cond, update_map, inc_map)

@perf_logging
def find_and_modify_url_info_md5(url, md5_hash):
    """
    Enabled cache
    update md5 fields
    """

    update_map = {"md5" : md5_hash}
    inc_map = None
    fields = ["md5"]

    url_info = UrlCacheClient.find_and_modify_url_info_by_not_md5(url, md5_hash, update_map, inc_map, fields)

    cond = default_cond(url)
    cond["md5"] = {"$ne" : md5_hash}

    if url_info is None:
        url_info =  _cond_update_url_info(cond, update_map, inc_map, fields)
    elif url_info == False:
        url_info = None
    else:
        _async_update_url_info(cond, update_map, inc_map)

    if url_info is None:
        return 0 #duplicate md5
    elif url_info["md5"] is not None:
        return 1 #md5 changed
    else:
        return 2 #first md5

global_db_caches = {}

def _get_results_by_cache(cache_key, query_func, force, *args):
    now = datetime.datetime.utcnow()
    if not force and global_db_caches.has_key(cache_key) and \
        now - global_db_caches[cache_key]["last_retrieved_time"] < datetime.timedelta(seconds = common_settings.db_cache_expiry_duration):
        results = global_db_caches[cache_key]["results"]
    else:
        results = misc.cursor_to_array(query_func(*args))
        db_cache = {"last_retrieved_time" : now, "results" : results}
        global_db_caches[cache_key] = db_cache
    return results

def _get_crawl_domain_infos(*args):
    domain_type = args[0]
    return db.crawlDomainWhitelist.find({"domain_type" : domain_type})

def get_crawl_domain_infos(domain_type = "full_domain", force = False):
    return  _get_results_by_cache("crawlDomainWhitelist_" + domain_type, _get_crawl_domain_infos, force, domain_type)

@perf_logging
def get_crawl_domain_info(domain, domain_type = "full_domain", force = False):
    results =  _get_results_by_cache("crawlDomainWhitelist_" + domain_type, _get_crawl_domain_infos, force, domain_type)
    results = filter(lambda domain_info : domain_info["domain"] == domain, results)
    return results[0] if len(results) > 0 else None

def _get_mobile_url_patterns(*args):
    return db.mobileUrlPatterns.find()

@perf_logging
def get_mobile_url_patterns(force = False):
    return  _get_results_by_cache("mobileUrlPatterns", _get_mobile_url_patterns, force)

def _get_negative_domains(*args):
    results = db.crawlDomainBlacklist.find()
    return map(lambda result : result["full_domain"], results)

@perf_logging
def get_negative_domains(force = False):
    return  _get_results_by_cache("crawlDomainBlacklist", _get_negative_domains, force)

#below are offline apis
@perf_logging
def save_crawl_domain_info(url, domain_type = "full_domain", crawl_priority = -1, crawl_depth = -1, \
    recrawl_details = False, recrawl_list = False, recrawl_undefined = False):#-1 means auto config needed

    domain_info = misc.get_url_domain_info(url)
    domain_types = common_settings.domain_types
    domain = domain_info[domain_types.index(domain_type)]
    update_map = {"domain" : domain, "domain_type" : domain_type, "url" : url,
        "crawl_priority" : crawl_priority, "crawl_depth" : crawl_depth,
        "recrawl_details" : recrawl_details, "recrawl_list" : recrawl_list, "recrawl_undefined" : recrawl_undefined,
        "_id" :  misc.md5(''.join([domain, domain_type]))
    }

    db.crawlDomainWhitelist.save(update_map)#Note: will override duplicate domain

@perf_logging
def get_url_infos_by_statuses(statuses, fields):
    fields = make_fields(fields)
    return db.urlRepository.find({"crawl_status" : {"$in" : statuses}}, fields=fields)

@perf_logging
def get_raw_docs_by_statuses(statuses, fields):
    fields = make_fields(fields)
    return db.rawDocs.find({"process_status" : {"$in" : statuses}}, fields=fields)

@perf_logging
def update_raw_doc(url, update_map):
    db.rawDocs.update(default_cond(url), update_map)

@perf_logging
def get_url_infos(cond, fields):
    fields = make_fields(fields)
    return db.urlRepository.find(cond, fields=fields)

@perf_logging
def save_offline_manipulation(manipulation, result, type):
    now = datetime.datetime.now()
    db.offlineManipulations.save({"_id" : misc.md5(str(now)), "manipulation" : manipulation, "result" : result, "datetime" : now, "type" : type})

@perf_logging
def save_negative_domain(full_domain):
    db.crawlDomainBlacklist.save({"full_domain" : full_domain})

@perf_logging
def save_mobile_url_pattern(regex):
    db.mobileUrlPatterns.save({"regex" : regex})
