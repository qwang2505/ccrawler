'''
Created on Jun 20, 2012

@author: fli
'''

import logging
import copy
import ccrawler.common.settings as common_settings
import ccrawler.utils.misc as misc
import lxml.html as p

from ccrawler.db.utils import perf_logging, NO_ROW_ID, configure_db, cursor_to_array, default_cond
from datetime import datetime
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError
from ccrawler.utils.format import datetime2timestamp

_db = None

logger = logging.getLogger('transcode.transcodedb')

_INDEXES = {
    'clients' : [
        'token',
    ],
    'results' : [
        [('bad', ASCENDING), ('_id', ASCENDING) ],
    ],
    'urlRedirects' : [
    ],
    'msites' : [
        'version',
        'url'
    ],
}

def config(server="localhost", port=27017, database="crawler", dedicated_configs={}):
    global _db
    _db = configure_db(server, port, database, _INDEXES, dedicated_configs)

if _db is None:
    config(common_settings.transcode_db_config.get("database_server", "localhost"),
           common_settings.transcode_db_config.get("database_port", 27017),
           common_settings.transcode_db_config.get("database_name", "transcode"),
           common_settings.transcode_db_config.get("dedicated_configs", {}))

_TYPE_STATUSCODE_DICT = {
    'details': 200,
    'list': 201
}

_DEFAULT_STATUSCODE = 200
_DEFAULT_RESULT_ORDER = [('_id', ASCENDING)]
_CONTENT_COLUMN_NAME = 'content_%d'

@perf_logging
def update_transcode_result(result, pages, page_type='details', process_type='batch'):
    i = 1
    for page in pages:
        content = ''
        for node in page:
            content += p.tostring(node)
        result[_CONTENT_COLUMN_NAME%i] = content
        i += 1
    result['statusCode'] = _TYPE_STATUSCODE_DICT[page_type] if page_type in _TYPE_STATUSCODE_DICT else _DEFAULT_STATUSCODE
    result['_id'] = misc.md5(result['url'])
    result['processType'] = process_type
    cond = {'_id': result['_id']}
    update = result
    _db.results.update(cond, update, upsert=True)

_RESULT_META_FIELDS = {
    'url':1,
    'head':1,
    'style':1,
    'pageCount':1,
    'statusCode':1,
}

@perf_logging
def get_result_by_url(url, start_index=1, page_type=1):
    cond = {'_id': misc.md5(url)}
    fields = NO_ROW_ID
    if page_type == 1:
        #only query specific page:
        fields = copy.copy(_RESULT_META_FIELDS)
        fields[_CONTENT_COLUMN_NAME % start_index] = 1
    return _db.results.find_one(cond, fields=fields)

_RESULT_INFO_FIELDS = {
    'url':1,
}

@perf_logging
def get_result_by_statusCode(statusCode=_DEFAULT_STATUSCODE, skip=0, limit=20, bad_page=None):
    cond = {'statusCode':statusCode, 'bad':bad_page}
    cursor = _db.results.find(cond, fields=_RESULT_INFO_FIELDS, sort=_DEFAULT_RESULT_ORDER, skip=skip*limit, limit=limit)
    return cursor_to_array(cursor)

@perf_logging
def update_result(url, update):
    cond = default_cond(url)
    update = {'$set':update}
    _db.results.update(cond, update)

@perf_logging
def save_redirect_url(url, redirect_url):
    now = datetime2timestamp(datetime.utcnow())
    _db.urlRedirects.save({"_id" : misc.md5(url), "url" : url, "redirect_url" : redirect_url, "created_time" : now})

@perf_logging
def get_redirect_url(url):
    redirect_info = _db.urlRedirects.find_one(default_cond(url), fields={"redirect_url" : 1})
    return redirect_info["redirect_url"] if redirect_info is not None else None

@perf_logging
def remove_redirect_url(url):
    _db.urlRedirects.remove(default_cond(url))

_MOBILE_SITES_FIELDS = {
    '_id' : 0,
    'url' : 1,
    'murl' : 1,
    'version' : 1,
    'operation' : 1
}

@perf_logging
def get_mobile_sites(version):
    cond = {'version' : {'$gt' : version}}
    cursor = _db.msites.find(cond, fields=_MOBILE_SITES_FIELDS)
    return cursor_to_array(cursor)

@perf_logging
def get_optimize_args(last_modify=None):
    '''
    Get optimise args from db. If last_modify provided, return args
    modified after last_modify, else return None.
    '''
    cond = {}
    if last_modify is not None:
        cond['lastModify'] = {'$gt': last_modify}
    return _db.optimizeArgs.find_one(cond)

@perf_logging
def get_site_templates(last_modify=None):
    '''
    Get site templates modified after last_modify. If last_modify is
    None, return all site templates.
    '''
    cond = {}
    if last_modify is not None:
        cond['mt'] = {'$gt': last_modify}
    templates = _db.siteTemplates.find(cond, NO_ROW_ID)
    return cursor_to_array(templates)

@perf_logging
def save_cache_urls(urls):
    '''
    Save cache urls in db.
    '''
    for url in urls:
        try:
            _db.cachedUrls.insert({'_id': url})
        except DuplicateKeyError:
            pass
