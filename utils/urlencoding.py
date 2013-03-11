'''
Created on Apr 26, 2011

@author: chzhong

Oringinal source code from https://github.com/nshah/python-urlencoding
'''
import sys
import collections
import urllib

from ccrawler.utils.format import is_nonstring_iterable, SortedDict

def escape(value, plus=True, safe='/'):
    """
    Escape the string according to:

    RFC3986: http://tools.ietf.org/html/rfc3986
    http://oauth.net/core/1.0/#encoding_parameters

    Arguments:

        `value`
            The string to escape.

        'plus'
            If true, spaces will be encoded as '+',
            otherwise will be encoded as '%20'.
            Default value if True.

        'safe'
            Other characters not to encode.
            Default value is '/'.

    >>> urlencoding.escape('a b & c', False)
    'a%20b%20%26%20c'
    >>> urlencoding.escape('abc123-._~', safe='~')
    'abc123-._~'

    """
    m = urllib.quote_plus if plus else urllib.quote
    return m(value, safe=safe)

def unscape(value, plus=True):
    """
    Unscape the string according to:

    RFC3986: http://tools.ietf.org/html/rfc3986
    http://oauth.net/core/1.0/#encoding_parameters

    Arguments:

        `value`
            The string to unscape.

         'plus'
            If true, '+' will be unspace as ' '.
            Default value if True.


    >>> urlencoding.unscape('a+b+%26+c')
    'a b & c'
    """
    m = urllib.unquote_plus if plus else urllib.unquote
    return m(value)

def encode(value, enc='utf-8', plus=True, safe=''):
    """
    Encode the string according to:

    RFC3986: http://tools.ietf.org/html/rfc3986
    http://oauth.net/core/1.0/#encoding_parameters

    Arguments:

        `value`
            The string to encode.

        'plus'
            If true, spaces will be encoded as '+',
            otherwise will be encoded as '%20'.
            Default value if True.

        'safe'
            Other characters not to encode.
            Default value is '/'.

        'enc'
           The encoding of the encoded value for non-ascii characters.
           Default value if 'utf-8'.

    >>> urlencoding.encode('a b & c')
    'a+b+%26+c'

    """
    if not isinstance(value, basestring):
        value = str(value)
    if enc:
        try:
            if isinstance(value, str):
                # Ensure we are handling a unicode object.
                value = value.decode('utf-8')
            # Encode the value using specified encoding.
            value = value.encode(enc)
        except UnicodeDecodeError:
            pass
    m = urllib.quote_plus if plus else urllib.quote
    return m(value, safe=safe)

def decode(value, enc='utf-8', plus=True):
    """
    Encode the string according to:

    RFC3986: http://tools.ietf.org/html/rfc3986
    http://oauth.net/core/1.0/#encoding_parameters

    Arguments:

        `value`
            The string to encode.

        'plus'
            If true, spaces will be encoded as '+',
            otherwise will be encoded as '%20'.
            Default value if True.

        'safe'
            Other characters not to encode.
            Default value is '/'.

        'enc'
           The encoding of the encoded value for non-ascii characters.
           Default value if 'utf-8'.

    >>> urlencoding.encode('a b & c')
    'a+b+%26+c'

    """
    if isinstance(value, unicode):
        value = value.decode('utf-8')
    m = urllib.unquote_plus if plus else urllib.unquote
    result = m(value)
    import codecs
    if enc and codecs.lookup(enc) != codecs.lookup('utf_8'):
        try:
            # Convert the encoding back to 'utf-8'.
            result = result.decode(enc)
            result = result.encode('utf-8')
        except UnicodeDecodeError:
            pass
    return result

def parse_qs(qs, enc='utf-8', keep_blank_values=0, strict_parsing=0):
    """Parse a query given as a string argument.

        Arguments:

        qs: URL-encoded query string to be parsed

        keep_blank_values: flag indicating whether blank values in
            URL encoded queries should be treated as blank strings.
            A true value indicates that blanks should be retained as
            blank strings.  The default false value indicates that
            blank values are to be ignored and treated as if they were
            not included.

        strict_parsing: flag indicating what to do with parsing errors.
            If false (the default), errors are silently ignored.
            If true, errors raise a ValueError exception.
    """
    dict = {}
    for name, value in parse_qsl(qs, enc, keep_blank_values, strict_parsing):
        if name in dict:
            dict[name].append(value)
        else:
            dict[name] = [value]
    # Make single value plattern
    for k, v in dict.iteritems():
        if len(v) == 1:
            dict[k] = v[0]
    return dict

def parse_qsl(qs, enc='utf-8', keep_blank_values=0, strict_parsing=0):
    """Parse a query given as a string argument.

    Arguments:

    qs: URL-encoded query string to be parsed

    keep_blank_values: flag indicating whether blank values in
        URL encoded queries should be treated as blank strings.  A
        true value indicates that blanks should be retained as blank
        strings.  The default false value indicates that blank values
        are to be ignored and treated as if they were  not included.

    strict_parsing: flag indicating what to do with parsing errors. If
        false (the default), errors are silently ignored. If true,
        errors raise a ValueError exception.

    Returns a list, as G-d intended.
    """
    pairs = [s2 for s1 in qs.split('&') for s2 in s1.split(';')]
    r = []
    for name_value in pairs:
        if not name_value and not strict_parsing:
            continue
        nv = name_value.split('=', 1)
        if len(nv) != 2:
            if strict_parsing:
                raise ValueError, "bad query field: %r" % (name_value,)
            # Handle case of a control-name with no equal sign
            if keep_blank_values:
                nv.append('')
            else:
                continue
        if len(nv[1]) or keep_blank_values:
            name = decode(nv[0], enc=enc)
            value = decode(nv[1], enc=enc)
            r.append((name, value))
    return r

ENCODED_OPEN_BRACKET = escape('[')
ENCODED_CLOSE_BRACKET = escape(']')


def urlencode(query, enc='utf-8', sort=False, doseq=1):
    """Encode a sequence of two-element tuples or dictionary into a URL query string.

    If any values in the query arg are sequences and doseq is true, each
    sequence element is converted to a separate parameter.

    If the query arg is a sequence of two-element tuples, the order of the
    parameters in the output will match the order of parameters in the
    input.
    """

    if hasattr(query, "iteritems"):
        # mapping objects
        if sort:
            query = SortedDict(query)
        query = query.iteritems()
    else:
        # it's a bother at times that strings and string-like objects are
        # sequences...
        try:
            # non-sequence items should not work with len()
            # non-empty strings will fail this
            if len(query) and not isinstance(query[0], tuple):
                raise TypeError
            # zero-length sequences of all types will get here and succeed,
            # but that's a minor nit - since the original implementation
            # allowed empty dicts that type of behavior probably should be
            # preserved for consistency
            if sort:
                query = sorted(query, lambda x, y: cmp(x[0], y[0]))
        except TypeError:
            _, _, tb = sys.exc_info()
            raise TypeError, "not a valid non-string sequence or mapping object", tb

    l = []
    if not doseq:
        # preserve old behavior
        for k, v in query:
            k = encode(k, safe='', enc=enc)
            v = encode(v, safe='', enc=enc)
            l.append(k + '=' + v)
    else:
        for k, v in query:
            k = encode(k, safe='', enc=enc)
            if isinstance(v, (str, unicode)):
                v = encode(v, safe='', enc=enc)
                l.append(k + '=' + v)
            else:
                try:
                    # is this a sufficient test for sequence-ness?
                    _ = len(v)
                except TypeError:
                    # not a sequence
                    v = encode(v, safe='')
                    l.append(k + '=' + v)
                else:
                    # loop over the sequence
                    for elt in v:
                        l.append(k + '=' + encode(elt, safe='', enc=enc))
    return '&'.join(l)

def compose_qs(params, sort=False, pattern='%s=%s', join='&', wrap=None):
    """
    Compose a single string using RFC3986 specified escaping using
    `urlencoding.escape`_ for keys and values.

    Arguments:

        `params`
            The dict of parameters to encode into a query string.

        `sort`
            Boolean indicating if the key/values should be sorted.

    >>> urlencoding.compose_qs({'a': '1', 'b': ' c d'})
    'a=1&b=%20c%20d'
    >>> urlencoding.compose_qs({'a': ['2', '1']})
    'a=2&a=1'
    >>> urlencoding.compose_qs({'a': ['2', '1', '3']}, sort=True)
    'a=1&a=2&a=3'
    >>> urlencoding.compose_qs({'a': '1', 'b': {'c': 2, 'd': 3}}, sort=True)
    'a=1&b%5Bc%5D=2&b%5Bd%5D=3'

    """

    if sort:
        params = SortedDict(params)

    pieces = []
    for key, value in params.iteritems():
        escaped_key = escape(str(key))
        if wrap:
            escaped_key = wrap + ENCODED_OPEN_BRACKET + escaped_key + ENCODED_CLOSE_BRACKET

        if isinstance(value, collections.Mapping):
            p = compose_qs(value, sort, pattern, join, escaped_key)
        elif is_nonstring_iterable(value):
            p = join.join([pattern % (escaped_key, escape(str(v))) for v in value])
        else:
            p = pattern % (escaped_key, escape(str(value)))
        pieces.append(p)
    return join.join(pieces)
