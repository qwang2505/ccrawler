'''
Created on Jun 19, 2012

@author: fli
'''
import simplejson
import time
import os
import hashlib
import calendar
import datetime
import collections

## {{{ http://code.activestate.com/recipes/576693/ (r9)
# Backport of OrderedDict() class that runs on Python 2.4, 2.5, 2.6, 2.7 and pypy.
# Passes Python2.7's test suite and incorporates all the latest updates.

try:
    from thread import get_ident as _get_ident
except ImportError:
    from dummy_thread import get_ident as _get_ident

try:
    from _abcoll import KeysView, ValuesView, ItemsView
except ImportError:
    pass


class ReadOnlyDict(dict):
    '''
    A dictionary that is read-only.
    '''

    def mutatableCopy(self):
        '''
        Return a mutable copy of this property table.
        '''
        return dict(self)

    def __setattr__(self, name, value):
        pass

    def __delattr__(self, name):
        pass

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

class PropertyTable(dict):
    '''
    A property table that allows create/get/set property that is not in the property list by using attribute syntax.
    '''

    @classmethod
    def fromJson(s, encoding=None, cls=None, object_hook=None, parse_float=None,
    parse_int=None, parse_constant=None, object_pairs_hook=None,
    use_decimal=False, **kw):
        '''
        Creates an PropertyTable from a JSON string.
        '''
        if s:
            d = simplejson.loads(s, encoding=encoding, cls=cls, object_hook=object_hook, parse_float=parse_float, parse_int=parse_int, parse_constant=parse_constant, object_pairs_hook=object_pairs_hook, use_decimal=use_decimal, **kw)
            return PropertyTable(d)
        else:
            return PropertyTable()

    def __getattr__(self, name):
        '''
        Delegate self.name to self[name]. If name not in self, None is returned.
        '''
        if name in self:
            return self[name]
        else:
            return self.getdefault(name)

    def getdefault(self, name):
        '''
        Retrieve the default value of a attribute.
        '''
        base = self.__dict__
        if '__defaults__' in base:
            defaults = base['__defaults__']
            if name in defaults:
                return defaults[name]
        return None

    def setdefault(self, name, value):
        '''
        Retrieve the default value of a attribute.
        '''
        base = self.__dict__
        if '__defaults__' in base:
            defaults = base['__defaults__']
        else:
            defaults = {}
            base['__defaults__'] = defaults
        defaults[name] = value

    def __setattr__(self, name, value):
        '''
        Delegate self.name = value to self[name] = value
        '''
        self[name] = value

    def __delattr__(self, name):
        '''
        Delegate the 'remove' action.
        '''
        del self[name]

    def readOnlyCopy(self):
        '''
        Return a read-only copy of this property table.
        '''
        return ReadOnlyPropertyTable(self)

    def tojson(self, skipkeys=False, ensure_ascii=True, check_circular=True,
        allow_nan=True, cls=None, indent=None, separators=None,
        encoding='utf-8', default=None, use_decimal=False, **kw):
        '''
        Convert the property table to a JSON string.
        '''
        return simplejson.dumps(self, skipkeys=skipkeys, ensure_ascii=ensure_ascii, check_circular=check_circular, allow_nan=allow_nan, cls=cls, indent=indent, separators=separators, encoding=encoding, default=default, use_decimal=use_decimal, **kw)


class ReadOnlyPropertyTable(PropertyTable):
    '''
    A read-only property table which attributes or properties can not be changed.
    '''

    def mutatableCopy(self):
        '''
        Return a mutable copy of this property table.
        '''
        return PropertyTable(self)

    def setdefault(self, name, value):
        pass

    def __setattr__(self, name, value):
        pass

    def __delattr__(self, name):
        pass

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

class OrderedDict(dict):
    'Dictionary that remembers insertion order'
    # An inherited dict maps keys to values.
    # The inherited dict provides __getitem__, __len__, __contains__, and get.
    # The remaining methods are order-aware.
    # Big-O running times for all methods are the same as for regular dictionaries.

    # The internal self.__map dictionary maps keys to links in a doubly linked list.
    # The circular doubly linked list starts and ends with a sentinel element.
    # The sentinel element never gets deleted (this simplifies the algorithm).
    # Each link is stored as a list of length three:  [PREV, NEXT, KEY].

    def __init__(self, *args, **kwds):
        '''Initialize an ordered dictionary.  Signature is the same as for
        regular dictionaries, but keyword arguments are not recommended
        because their insertion order is arbitrary.

        '''
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))
        try:
            self.__root
        except AttributeError:
            self.__root = root = []                     # sentinel node
            root[:] = [root, root, None]
            self.__map = {}
        self.__update(*args, **kwds)

    def __setitem__(self, key, value, dict_setitem=dict.__setitem__):
        'od.__setitem__(i, y) <==> od[i]=y'
        # Setting a new item creates a new link which goes at the end of the linked
        # list, and the inherited dictionary is updated with the new key/value pair.
        if key not in self:
            root = self.__root
            last = root[0]
            last[1] = root[0] = self.__map[key] = [last, root, key]
        dict_setitem(self, key, value)

    def __delitem__(self, key, dict_delitem=dict.__delitem__):
        'od.__delitem__(y) <==> del od[y]'
        # Deleting an existing item uses self.__map to find the link which is
        # then removed by updating the links in the predecessor and successor nodes.
        dict_delitem(self, key)
        link_prev, link_next, key = self.__map.pop(key)
        link_prev[1] = link_next
        link_next[0] = link_prev

    def __iter__(self):
        'od.__iter__() <==> iter(od)'
        root = self.__root
        curr = root[1]
        while curr is not root:
            yield curr[2]
            curr = curr[1]

    def __reversed__(self):
        'od.__reversed__() <==> reversed(od)'
        root = self.__root
        curr = root[0]
        while curr is not root:
            yield curr[2]
            curr = curr[0]

    def clear(self):
        'od.clear() -> None.  Remove all items from od.'
        try:
            for node in self.__map.itervalues():
                del node[:]
            root = self.__root
            root[:] = [root, root, None]
            self.__map.clear()
        except AttributeError:
            pass
        dict.clear(self)

    def popitem(self, last=True):
        '''od.popitem() -> (k, v), return and remove a (key, value) pair.
        Pairs are returned in LIFO order if last is true or FIFO order if false.

        '''
        if not self:
            raise KeyError('dictionary is empty')
        root = self.__root
        if last:
            link = root[0]
            link_prev = link[0]
            link_prev[1] = root
            root[0] = link_prev
        else:
            link = root[1]
            link_next = link[1]
            root[1] = link_next
            link_next[0] = root
        key = link[2]
        del self.__map[key]
        value = dict.pop(self, key)
        return key, value

    # -- the following methods do not depend on the internal structure --

    def keys(self):
        'od.keys() -> list of keys in od'
        return list(self)

    def values(self):
        'od.values() -> list of values in od'
        return [self[key] for key in self]

    def items(self):
        'od.items() -> list of (key, value) pairs in od'
        return [(key, self[key]) for key in self]

    def iterkeys(self):
        'od.iterkeys() -> an iterator over the keys in od'
        return iter(self)

    def itervalues(self):
        'od.itervalues -> an iterator over the values in od'
        for k in self:
            yield self[k]

    def iteritems(self):
        'od.iteritems -> an iterator over the (key, value) items in od'
        for k in self:
            yield (k, self[k])

    def update(*args, **kwds): #@NoSelf
        '''od.update(E, **F) -> None.  Update od from dict/iterable E and F.

        If E is a dict instance, does:           for k in E: od[k] = E[k]
        If E has a .keys() method, does:         for k in E.keys(): od[k] = E[k]
        Or if E is an iterable of items, does:   for k, v in E: od[k] = v
        In either case, this is followed by:     for k, v in F.items(): od[k] = v

        '''
        if len(args) > 2:
            raise TypeError('update() takes at most 2 positional '
                            'arguments (%d given)' % (len(args),))
        elif not args:
            raise TypeError('update() takes at least 1 argument (0 given)')
        self = args[0]
        # Make progressively weaker assumptions about "other"
        other = ()
        if len(args) == 2:
            other = args[1]
        if isinstance(other, dict):
            for key in other:
                self[key] = other[key]
        elif hasattr(other, 'keys'):
            for key in other.keys():
                self[key] = other[key]
        else:
            for key, value in other:
                self[key] = value
        for key, value in kwds.items():
            self[key] = value

    __update = update  # let subclasses override update without breaking __init__

    __marker = object()

    def pop(self, key, default=__marker):
        '''od.pop(k[,d]) -> v, remove specified key and return the corresponding value.
        If key is not found, d is returned if given, otherwise KeyError is raised.

        '''
        if key in self:
            result = self[key]
            del self[key]
            return result
        if default is self.__marker:
            raise KeyError(key)
        return default

    def setdefault(self, key, default=None):
        'od.setdefault(k[,d]) -> od.get(k,d), also set od[k]=d if k not in od'
        if key in self:
            return self[key]
        self[key] = default
        return default

    def __repr__(self, _repr_running={}):
        'od.__repr__() <==> repr(od)'
        call_key = id(self), _get_ident()
        if call_key in _repr_running:
            return '...'
        _repr_running[call_key] = 1
        try:
            if not self:
                return '%s()' % (self.__class__.__name__,)
            return '%s(%r)' % (self.__class__.__name__, self.items())
        finally:
            del _repr_running[call_key]

    def __reduce__(self):
        'Return state information for pickling'
        items = [[k, self[k]] for k in self]
        inst_dict = vars(self).copy()
        for k in vars(OrderedDict()):
            inst_dict.pop(k, None)
        if inst_dict:
            return (self.__class__, (items,), inst_dict)
        return self.__class__, (items,)

    def copy(self):
        'od.copy() -> a shallow copy of od'
        return self.__class__(self)

    @classmethod
    def fromkeys(cls, iterable, value=None):
        '''OD.fromkeys(S[, v]) -> New ordered dictionary with keys from S
        and values equal to v (which defaults to None).

        '''
        d = cls()
        for key in iterable:
            d[key] = value
        return d

    def __eq__(self, other):
        '''od.__eq__(y) <==> od==y.  Comparison to another OD is order-sensitive
        while comparison to a regular mapping is order-insensitive.

        '''
        if isinstance(other, OrderedDict):
            return len(self) == len(other) and self.items() == other.items()
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not self == other

    # -- the following methods are only used in Python 2.7 --

    def viewkeys(self):
        "od.viewkeys() -> a set-like object providing a view on od's keys"
        return KeysView(self)

    def viewvalues(self):
        "od.viewvalues() -> an object providing a view on od's values"
        return ValuesView(self)

    def viewitems(self):
        "od.viewitems() -> a set-like object providing a view on od's items"
        return ItemsView(self)
## end of http://code.activestate.com/recipes/576693/ }}}



class SortedDict(dict):

    def __reversed__(self):
        'od.__reversed__() <==> reversed(od)'
        s = sorted(self)
        return reversed(s)

    def keys(self):
        'od.keys() -> list of keys in od'
        return list(sorted(self))

    def values(self):
        'od.values() -> list of values in od'
        return [self[key] for key in sorted(self)]

    def items(self):
        'od.items() -> list of (key, value) pairs in od'
        return [(key, self[key]) for key in sorted(self)]

    def iterkeys(self):
        'od.iterkeys() -> an iterator over the keys in od'
        return iter(sorted(self))

    def itervalues(self):
        'od.itervalues -> an iterator over the values in od'
        for k in sorted(self):
            yield self[k]

    def iteritems(self):
        """
        Iterates in a sorted fashion. Values are sorted before being yielded if
        they can be. It should result in sorted by key, then value semantics.

        http://oauth.net/core/1.0/#rfc.section.9.1.1

        """
        for key in sorted(self):
            value = self[key]
            if isinstance(value, collections.Mapping):
                value = SortedDict(value)
            elif is_nonstring_iterable(value):
                value = sorted(value)
            yield key, value


def _get_data(package, resource):
    import sys
    loader = pkgutil.get_loader(package)
    if loader is None or not hasattr(loader, 'get_data'):
        return None
    mod = sys.modules.get(package) or loader.load_module(package)
    if mod is None or not hasattr(mod, '__file__'):
        return None

    # Modify the resource name to be compatible with the loader.get_data
    # signature - an os.path format "filename" starting with the dirname of
    # the package's __file__
    parts = resource.split('/')
    parts.insert(0, os.path.dirname(mod.__file__))
    resource_name = os.path.join(*parts)
    return loader.get_data(resource_name)

# pkgutil.get_data() not available in python 2.5
# see http://docs.python.org/release/2.5/lib/module-pkgutil.html
try:
    import pkgutil
    get_data = pkgutil.get_data
except AttributeError:
    get_data = _get_data

ONE_DAY = datetime.timedelta(days=1)

def unixnow():
    '''
    Returns current UNIX timestamp in milliseconds.
    '''
    return int(time.time() * 1000)

_boolean_states = {'1': True, 'yes': True, 'true': True, 'on': True,
                       '0': False, 'no': False, 'false': False, 'off': False}

def boolean(x):
    '''
    If x is None, returns False.
    If x is a basestring, and its content is one of (1, yes, true, on) in any case, returns True.
    If x is a basestring, and its content is one of (0, no, false, off) in any case, returns False.
    If x is not a basestring, returns bool(x).
    '''
    if not x:
        return False
    if isinstance(x, basestring):
        if x.lower() not in _boolean_states:
            raise ValueError, 'Not a boolean: %s' % x
        return _boolean_states[x.lower()]
    else:
        return bool(x)

def lower(s):
    '''
    Return the lower form of a given string.
    '''
    if not s:
        return s
    if isinstance(s, basestring):
        return s.lower()
    else:
        raise ValueError('s must be a str or unicode.')

def upper(s):
    '''
    Return the upper form of a given string.
    '''
    if not s:
        return s
    if isinstance(s, basestring):
        return s.upper()
    else:
        raise ValueError('s must be a str or unicode.')

def take_first(values):
    '''
    Take the first item from an collection.

    @param values: The collection to take from.
    @return: The first item that evaluates as 'True'.
    '''
    if isinstance(values, collections.Iterable):
        for value in values:
            if value:
                return value
        return None
    return values

def datetime2timestamp(dt):
    '''
    Converts a datetime object to UNIX timestamp in milliseconds.
    '''
    if hasattr(dt, 'utctimetuple'):
        t = calendar.timegm(dt.utctimetuple())
        timestamp = int(t) + dt.microsecond / 1000000.0
        return int(timestamp * 1000)
    return dt

def timestamp2datetime(timestamp):
    '''
    Converts UNIX timestamp in milliseconds to a datetime object.
    '''
    if isinstance(timestamp, (int, long, float)):
        return datetime.datetime.utcfromtimestamp(timestamp / 1000)
    return timestamp

def date_part(date):
    '''
    Return the date part of a given day.
    '''
    if date:
        return date.replace(hour=0, minute=0, second=0, microsecond=0)
    return None

def total_days(delta):
    '''
    Return the total days of an datetime.timedelta.
    @param delta: The time delta object.
    '''
    if delta is None:
        return 0
    if not isinstance(delta, datetime.timedelta):
        raise ValueError('delta must be an dateime.timedelta.')
    return delta.days + delta.seconds / 86400.0 + delta.microseconds / 86400000000.0

def total_seconds(delta):
    '''
    Return the total seconds of an datetime.timedelta.
    @param delta: The time delta object.
    '''
    if delta is None:
        return 0
    if not isinstance(delta, datetime.timedelta):
        raise ValueError('delta must be an dateime.timedelta.')
    return delta.days * 86400.0 + delta.seconds + delta.microseconds / 1000000.0

def total_microseconds(delta):
    '''
    Return the total microseconds of an datetime.timedelta.
    @param delta: The time delta object.
    '''
    if delta is None:
        return 0
    if not isinstance(delta, datetime.timedelta):
        raise ValueError('delta must be an dateime.timedelta.')
    return delta.days * 86400000000.0 + delta.seconds * 1000000.0 + delta.microseconds
def now():
    '''
    Return now.
    '''
    return datetime.datetime.now()

def today():
    '''
    Return the date part of today.
    '''
    return date_part(datetime.datetime.now())

def tomorrow():
    '''
    Return the date part of tomorrow.
    '''
    return today() + ONE_DAY

def yesterday():
    '''
    Return the date part of yesterday.
    '''
    return today() - ONE_DAY

HTTP_DATE_LOCALE = ('en_US', 'UTF8')
HTTP_DATE_FORMATS = [
    '%a, %d %b %Y %H:%M:%S GMT',
    '%A, %d-%b-%y %H:%M:%S GMT',
]

def parse_http_date(http_date):
    '''
    Parse an HTTP date into a datetime object.

    @param http_date: The text of the HTTP Date.
    '''
    if not http_date:
        return None
    import locale
    locale_cache = None
    # Ensure we use en_US locale to make abbreviated weekday, month correct.
    if locale.getlocale(locale.LC_TIME)[0] != HTTP_DATE_LOCALE[0]:
        locale_cache = locale.getlocale(locale.LC_ALL)
        locale.setlocale(locale.LC_TIME, HTTP_DATE_LOCALE)
    date = None
    for formatter in HTTP_DATE_FORMATS:
        try:
            date = datetime.datetime.strptime(http_date, formatter)
            break
        except ValueError:
            pass
    if not date:
        raise ValueError("'%s' isn't a valid HTTP date." % http_date)
    if locale_cache:
        locale.setlocale(locale.LC_TIME, locale_cache)
    return date

def to_http_date(date):
    '''
    Converts a datetime object to HTTP Date.

    @param date: The datetime object.
    '''
    if not date:
        return None
    if not isinstance(date, datetime.datetime):
        raise ValueError('%s is not a datetime.dateimte' % date)
    import locale
    locale_cache = None
    # Ensure we use en_US locale to make abbreviated weekday, month correct.
    if locale.getlocale(locale.LC_TIME)[0] != HTTP_DATE_LOCALE[0]:
        locale_cache = locale.getlocale(locale.LC_ALL)
        locale.setlocale(locale.LC_TIME, HTTP_DATE_LOCALE)
    http_date = date.strftime(HTTP_DATE_FORMATS[0])
    if locale_cache:
        locale.setlocale(locale.LC_TIME, locale_cache)
    return http_date


class FixedOffset(datetime.tzinfo):
    "Fixed offset in minutes east from UTC."
    def __init__(self, offset):
        if isinstance(offset, datetime.timedelta):
            self.__offset = offset
            offset = self.__offset.seconds // 60
        else:
            self.__offset = datetime.timedelta(minutes=offset)

        sign = offset < 0 and '-' or '+'
        self.__name = u"%s%02d%02d" % (sign, abs(offset) / 60., abs(offset) % 60)

    def __repr__(self):
        return self.__name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return datetime.timedelta(0)

def obj2json(obj):
    '''
    Converts an object to JSON string.
    '''
    return simplejson.dumps(obj)

def dict2PropertyTable(d, recursive=True):
    '''
    Converts an dict to PropertyTable.

    If the give paremeter is not a dict, it is returned directly.
    '''
    if d:
        if isinstance(d, dict) and not isinstance(d, PropertyTable):
            d = PropertyTable(d)
        if recursive:
            if isinstance(d, dict):
                for k in d:
                    d[k] = dict2PropertyTable(d[k])
            elif hasattr(d, '__iter__'):
                d = map(dict2PropertyTable, d)
    return d

def json2obj(s):
    '''
    Creates an PropertyTable from JSON string.
    '''
    d = simplejson.loads(s)
    return dict2PropertyTable(d)

def _checkoverflow(val, max_val):
    if abs(val) >= max_val:
        raise Exception('Value %d overflowed.' % val)

def unsigned(v, base=64):
    '''
    Convert a signed integer to unsigned integer of a given base.
    '''
    max_val = 1 << base
    _checkoverflow(v, max_val)
    if v < 0:
        v += max
    return v

def signed(v, base=64):
    '''
    Convert a unsigned integer to signed integer of a given base.
    '''
    max_val = 1 << base
    max_positive = 1 << (base - 1)
    max_negate = (1 << (base - 1)) + 1
    if v < 0:
        _checkoverflow(v, max_negate)
        return v
    if v > max_positive:
        _checkoverflow(v, max_val)
        v -= max
    return v


def ensuredir(directory):
    '''
    Ensure a certain directory exits.
    '''
    if not os.path.exists(directory):
        os.makedirs(directory)

def hashdigest(s, algorithm=None):
    '''
    Return hash digest of a given string, using specified algorithm or default hash.
    '''
    if isinstance(s, unicode):
        s = s.encode('utf-8')
    if algorithm:
        l = hashlib.new(algorithm)
        l.update(s)
        digest = l.hexdigest()
    else:
        digest = '%016x' % unsigned(hash(s))

    return digest

def md5(s):
    '''
    Return md5 digest of a given string.
    '''
    return hashdigest(s, algorithm='md5')

def sha1(s):
    '''
    Return sha1 digest of a given string.
    '''
    return hashdigest(s, algorithm='sha1')

def strip(s):
    '''strip and return result of a string is is not None. Otherwise return None.
    '''
    if s is not None and isinstance(s, basestring):
        s = s.strip()
    return s

def is_nonstring_iterable(i):
    return not isinstance(i, basestring) and isinstance(i, collections.Iterable)
