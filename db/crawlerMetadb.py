
from ccrawler.db.utils import configure_db
from ccrawler.db.utils import perf_logging, default_cond, make_fields
import ccrawler.common.settings as common_settings

db = None

_INDEXES = {
    'urlRepositoryMeta' : [
        'url',
        'crawl_status',
    ],
}

def config(server="localhost", port=27017, database="crawlerMeta", dedicated_configs={}):
    global db
    db = configure_db(server, port, database, _INDEXES, dedicated_configs)

if db is None:
    config(common_settings.crawlerMeta_db_config.get("database_server", "localhost"),
           common_settings.crawlerMeta_db_config.get("database_port", 27017),
           common_settings.crawlerMeta_db_config.get("database_name", "crawlerMeta"),
           common_settings.crawlerMeta_db_config.get("dedicated_configs", {}))

def _cond_get_url_info_meta(cond, fields):
    return db.urlRepositoryMeta.find(cond, fields)

@perf_logging
def insert_url_info_meta(update):
    db.urlRepositoryMeta.insert(update)

@perf_logging
def update_url_info_meta(cond, update):
    db.urlRepositoryMeta.update(cond, update)

@perf_logging
def get_url_info_meta(url, fields):
    cond = default_cond(url)
    fields = make_fields(fields)
    return _cond_get_url_info_meta(cond, fields)
