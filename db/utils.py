'''
Created on Jun 19, 2012

@author: fli
'''
import re
from time import time
import logging

from pymongo.son_manipulator import SON
from pymongo.connection import Connection
from pymongo import ASCENDING

from ccrawler.utils.format import dict2PropertyTable, ReadOnlyPropertyTable, ReadOnlyDict
import ccrawler.utils.misc as misc

NO_ROW_ID = ReadOnlyDict({'_id' : 0})
ALL_ROWS = ReadOnlyDict({})

_connections = {}

_CONN_RE = re.compile(r"(?P<hosts>(?P<host>[A-Z0-9_.-]+)(?P<portpart>:(?P<port>\d+))?(?P<repls>(?P<repl>,[A-Z0-9_.-]+(:\d+)?)*))/(?P<db>\w+)", re.IGNORECASE)

def parse_conn_string(conn_str):
    m = _CONN_RE.search(conn_str)
    if m:
        if m.group('repls'):
            host = m.group('hosts')
            port = None
        else:
            host = m.group('host')
            port = int(m.group('port')) if m.group('port') else 27017
        db = m.group('db')
        return ReadOnlyPropertyTable({
            'server' : host,
            'port' : port,
            'db' : db
        })
    else:
        raise ValueError('The connection string "%s" is incorrect.' % conn_str)

def connect(host, port=None):
    '''
    Connect to the database.
    '''
    assert host, 'host of the database server may not be null.'
    global _connections
    key = (host, port or 27017)
    conn = None
    if key in _connections:
        conn = _connections[key]
    else:
        conn = Connection(host, port)
#        if len(conn.nodes) > 1:
#            conn = ReplicaSetConnection(conn)
        _connections[key] = conn
    return conn

def disconnect(host, port=None):
    '''
    Connect from the database.
    '''
    assert host, 'host of the database server may not be null.'
    global _connections
    key = (host, port or 27017)
    if key in _connections:
        conn = _connections[key]
        conn.disconnect()
        del _connections[key]

def cursor_to_array(cursor):
    if not cursor:
        return None
    items = map(dict2PropertyTable, cursor)
    closeCursor(cursor)
    return items

def closeCursor(cursor):
    cursor.close

pref_logger = logging.getLogger('dolphin-transcode.core.db+prefs')

def perf_logging(func):
    """
    Record the performance of each method call.

    Also catches unhandled exceptions in method call and response a 500 error.
    """
    def pref_logged(*args, **kwargs):
        argnames = func.func_code.co_varnames[:func.func_code.co_argcount]
        fname = func.func_name
        msg = 'DB - -> %s(%s)' % (fname, ','.join('%s=%s' % entry for entry in zip(argnames[1:], args[1:]) + kwargs.items()))
        startTime = time()
        retVal = func(*args, **kwargs)
        endTime = time()
        pref_logger.debug('%s <- %s ms.' % (msg, 1000 * (endTime - startTime)))
        return retVal
    return pref_logged

class IncrementalId(object):
    """implement incremental id for collection in mongodb
    """
    def __init__(self, db):
        self.db = db
        self.colls = {}

    def _ensure_next_id(self, coll_name):
        """ensure next_id item in collection ,if not, next_id method will throw exception rasie by pymongo"""
        cond = {'_id':coll_name}
        id_info = self.db.ids.find_one(cond)
        if  not id_info:
            self.db.ids.insert({'_id':coll_name, 'seq':1L})

    def next_test_id(self, coll, key):
        """get next increment id and increase it """
        item = self.db[coll].find_one({}, fields={ key : 1, '_id' : 0}, sort=[(key, ASCENDING)])
        if item:
            itemId = item[key]
            return itemId - 1
        else:
            return 0

    def next_id(self, coll):
        """get next increment id and increase it """
        if coll not in self.colls:
            self._ensure_next_id(coll)
        cond = {'_id':coll}
        update = {'$inc':{'seq':1L}}
        son = SON([('findandmodify', 'ids'), ('query', cond), ('update', update), ('new', True)])
        seq = self.db.command(son)
        return seq['value']['seq']

def config(module, server, port=None, db=None):
    '''
    Configure a data access module.
    '''
    assert server and db, 'Either "server" or "db" may not be None.'
    module._conn = connect(server, port)
    module._db = module._conn[db]
    module._ids = IncrementalId(module._db)
    if hasattr(module, '_INDEXES'):
        ensure_indexes(module, module._INDEXES)

def ensure_index(module, collection_name, indexes):
    '''
    Ensure a data access module's collection indexes.
    '''
    collection = module._db[collection_name]
    for index in indexes:
        collection.ensure_index(index)

def ensure_indexes(module, index_table):
    '''
    Ensure a data access module's indexes.
    '''
    for collection, indexes in index_table.items():
        ensure_index(module, collection, indexes)

class MultiDb(object):
    def __getattr__(self, collection_name):
        return self._get_collection(collection_name)

    def __getitem__(self, collection_name):
        return self._get_collection(collection_name)

    def _get_collection(self, collection_name):
        if self._collections.has_key(collection_name):
            return self._collections[collection_name]
        else:
            collection = self._default_db[collection_name]
            self._collections[collection_name] = collection
            return collection

def _get_connection(conn_pool, server, port):
    key = server + ":" + str(port)
    if conn_pool.has_key(key):
        return conn_pool[key]
    else:
        connection = Connection(server, port)
        conn_pool[key] = connection
        return connection

def _ensure_indexes(db, indexes):
    for collection, indexes in indexes.items():
        for index in indexes:
            db[collection].ensure_index(index)

def configure_db(server, port, database, indexes, dedicated_configs={}):
    conn_pool = {}
    collections = {}

    connection = _get_connection(conn_pool, server, port)
    default_db = connection[database]
    for collection_name, config in dedicated_configs.items():
        dedicated_server = config.get("database_server", server)
        dedicated_port = config.get("database_port", port)
        connection = _get_connection(conn_pool, dedicated_server, dedicated_port)
        dedicated_database = config.get("database_name", database)
        db = connection[dedicated_database]
        collection = db[collection_name]
        collections[collection_name] = collection

    db = MultiDb()
    db._conn_pool = conn_pool
    db._default_db = default_db
    db._collections = collections

    _ensure_indexes(db, indexes)

    return db

def default_cond(url):
    return {"_id" : misc.md5(url)}

def make_fields(fields):
    fields= dict(zip(fields, [1 for _ in range(len(fields))]))
    #if not fields.has_key("_id"): #disable _id output for perf
    #    fields["_id"] = 0
    return fields
