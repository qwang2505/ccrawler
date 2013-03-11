import re
import chardet
import codecs
from BeautifulSoup import BeautifulSoup

from ccrawler.utils.log import logging

_CONTENT_TYPE_META_REG_= re.compile('<\s*meta[^>]+charset=\"?([^>]+?)[;\'\">]', re.I)

def _get_encoding_from_header(headers, body):
    content_type = headers['Content-Type']
    if content_type is not None and (isinstance(content_type, str) or isinstance(content_type, unicode)):
        charset=BeautifulSoup.CHARSET_RE.search(content_type)
    else:
        charset = None
    return charset and charset.group(3) or None

def _get_encoding_from_meta(headers, body):
    match=_CONTENT_TYPE_META_REG_.search(body[0:1024])
    if match is not None:
        encoding = match.group(1).strip().lower()
        if len(encoding) == 0:
            return None
        else:
            return encoding
    else:
        return None

def _get_encoding_by_chardet(headers, body):
    return chardet.detect(body)['encoding']

def _try_get_encoding(headers, body, try_count):
    attempts = [_get_encoding_from_header, _get_encoding_from_meta,
                lambda headers, body : "GBK",
                lambda headers, body : "gb2312",
                _get_encoding_by_chardet,
                lambda headers, body : "utf-8", lambda headers, body : None]

    encoding = None
    for i in range(try_count, len(attempts)):
        encoding = attempts[i](headers, body)
        if encoding is not None:
            return encoding, i
    return None, len(attempts)

def _try_decode(url, body, encode):
    html = None
    try:
        decoder = codecs.lookup(encode)
        html = decoder.decode(body)[0]
    except Exception:
        logging.debug("try decode failed", encoding = encode,url = url)
    return html

def decode(url, headers, body, encoding=None):
    '''
    decode html to unicode
    '''

    try_count = 0

    while True:
        if encoding is not None:
            try_count = -1
        else:
            encoding, try_count = _try_get_encoding(headers, body, try_count)
            if encoding is None:
                logging.error("decoding failed for url", url)
                return None, None
        html = _try_decode(url, body, encoding)
        if html is not None:
            logging.debug('decode url succeeded', url = url, encoding = encoding, try_count = try_count)
            return html, encoding
        else:
            try_count += 1
            encoding = None

def decode_string(string, encoding=None):
    if string is None:
        return None
    if isinstance(string, unicode):
        return string

    if not isinstance(string, str):
        raise Exception("just support decode string")

    try:
        if encoding is None:
            encoding = "utf-8"
        return string.decode(encoding)
    except UnicodeDecodeError:
        encoding = chardet.detect(string)['encoding']
        try:
            decoder = codecs.lookup(encoding)
            return decoder.decode(string, 'ignore')[0]
        except Exception:
            return None

def encode_string(string, encoding=None):
    if string is None:
        return None
    if isinstance(string, str):
        return string
    if not isinstance(string, unicode):
        raise Exception("just support encode unicode")

    if encoding is None:
        encoding = "utf-8"

    try:
        return string.encode("utf-8")
    except UnicodeEncodeError:
        encoding = chardet.detect(string)['encoding']
        try:
            encoder = codecs.lookup(encoding)
            return encoder.encode(string, 'ignore')[0]
        except Exception:
            return None
