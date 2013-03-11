import urlparse
import sys
import socket
import hashlib
import urllib2
import cStringIO
import gzip
import os
#import pymongo
import string
import xml.dom.minidom
import simplejson

import ccrawler.utils.decoder as decoder
import ccrawler.utils.page_parser as page_parser
import ccrawler.utils.tldextract as tldextract

def load_object(path):
    """Load an object given its absolute object path, and return it.

    object can be a class, function, variable o instance.
    path ie: 'scrapy.contrib.downloadermiddelware.redirect.RedirectMiddleware'
    """

    try:
        dot = path.rindex('.')
    except ValueError:
        raise ValueError, "Error loading object '%s': not a full path" % path

    module, name = path[:dot], path[dot+1:]
    try:
        mod = __import__(module, {}, {}, [''])
    except ImportError, e:
        raise ImportError, "Error loading object '%s': %s" % (path, e)

    try:
        obj = getattr(mod, name)
    except AttributeError:
        raise NameError, "Module '%s' doesn't define any object named '%s'" % (module, name)

    return obj

def section_join(first, second):
    if len(first) == 0:
        return second
    elif len(second) == 0:
        return first
    else:
        return first + "." + second

def get_url_domain_info(url):
    subdomain, domain, tld = tldextract.extract(url)

    full_domain = section_join(domain, tld)
    host = section_join(subdomain, full_domain)
    return domain, full_domain, host

def process_file(filename, func):
    with open(filename, "r") as f:
        for line in f:
            if len(line) > 0 and line[-1] == '\r':
                line = line[:-1]
            if len(line) > 0 and line[-1] == '\n':
                line = line[:-1]
            func(line)

def load_file(filename):
    rows = []
    def append_row(line):
        line = line.decode("utf-8")
        rows.append(line)

    process_file(filename, append_row)
    return rows

def write_file(filename, rows):
    with open(filename, "w") as f:
        for row in rows:
            row = row.encode("utf-8")
            f.write(row + "\n")

def clone_dict(source, fields, soft = False):
    target = {}
    return copy_dict(source, target, fields, soft)

def copy_dict(source, target, fields, soft = False):
    for field in fields:
        if not soft:
            target[field] = source[field]
        elif source.has_key(field):
            target[field] = source[field]

    return target

def copy_dict_cond(source, target, cond):
    for key, value in source.items():
        if cond(key, value):
            target[key] = value

    return target

def load_body(url, encoding=None):
    try:
        req = urllib2.Request(url)
        req.add_header('User-Agent', "Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10")
        response=urllib2.urlopen(req)
        ce=response.headers.get('Content-Encoding',None)
        if ce and ce.lower().find('gzip')!=-1:
            body=cStringIO.StringIO(response.read())
            body=gzip.GzipFile(fileobj=body,mode='rb').read()
        else:
            body = response.read()
        content_type = response.headers.get('Content-Type',None)
        body, _ = decoder.decode(url,{'Content-Type':content_type},body, encoding=encoding)
        return body
    except:
        return None

def load_dom(url, encoding = None):
    body = load_body(url, encoding)
    if body == None:
        return None
    dom = page_parser.parse_unicode(body, url)
    return dom

def get_xml_element_value(elem, name):
    children = elem.getElementsByTagName(name)
    if len(children) == 0:
        return None

    return children[0].toxml().replace("<%s>" % name,"").replace("</%s>" % name,"")

ascii_space_table = string.maketrans("", "")
unicode_space_chars = u" \t\n\r\xa0\x0b\x0c\u3000"
unicode_space_table = dict((ord(char), None) for char in unicode_space_chars)

def remove_space(text):
    global space_chars
    global space_table
    if isinstance(text, str):
        return text.translate(ascii_space_table, string.whitespace)
    elif isinstance(text, unicode):
        return text.translate(unicode_space_table)
    else:
        raise Exception("not supported type")

def find_list(textOrFunc, found_list):
    for item in found_list:
        if isinstance(textOrFunc, str) or isinstance(textOrFunc, unicode):
            text = textOrFunc
            if text.find(item) != -1:
                return True
        else:
            func = textOrFunc
            if func(item):
                return True
    return False

def index_list(func, found_list):
    for i in range(len(found_list)):
        if func(found_list[i]):
            return i
    return -1

def column(md_list, i):
    return [row[i] for row in md_list]

def diff_seconds(first, second):
    delta = first - second
    return delta.days * 86400 + delta.seconds

def delta_seconds(delta):
    return delta.days * 86400 + delta.seconds

def load_user_agent(file_name):
    if os.path.isfile(file_name):
        return load_file(file_name)
    else:
        url = "http://www.user-agents.org/allagents.xml"
        body = load_body(url, encoding="utf-8")
        if body is not None:
            dom = xml.dom.minidom.parseString(body)
            user_agents = dom.getElementsByTagName("user-agent")
            crawler_agents = filter(lambda agent : get_xml_element_value(agent, "Type").find("R") != -1, user_agents)
            agent_names = map(lambda agent : get_xml_element_value(agent, "String"), crawler_agents)
        else:
            agent_names = []
        write_file(file_name, agent_names)
        return agent_names

def sum_func(func, iterable):
    return sum(map(func, iterable))

def sum_dict(key, dict_value):
    return sum_func(lambda item : item[key], dict_value)

def sum_obj(field, objects):
    return sum_func(lambda obj : obj.field, objects)

def add_dicts_field(iterable, key, func):
    def _add_dict(dict_value):
        dict_value[key] = func(dict_value)
        return dict_value

    return map(_add_dict, iterable)

def for_each(func, iterable):
    for item in iterable:
        func(item)

def count(iterable, key=None):
    def _count(group, item):
        if key is not None:
            item_key = key(item)
        else:
            item_key = item
        if group.has_key(item_key):
            group[item_key] += 1
        else:
            group[item_key] = 1
        return group

    return reduce(_count, iterable, {})

def md5(input_value):
    if isinstance(input_value, unicode):
        input_value = input_value.encode("utf-8")
    elif not isinstance(input_value, str):
        input_value = str(input_value)
    return hashlib.md5(input_value).hexdigest()

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('google.com', 0))
    ip = s.getsockname()[0]
    s.close()
    return ip

def cursor_to_array(cursor):
    return map(lambda item : item, cursor)

def select(dict_iterable, fields):
    return map(lambda dict: clone_dict(dict, fields), dict_iterable)

def distinct(iterable, key=None):
    cache = {}
    for item in iterable:
        if key is not None:
            item_key = key(item)
        else:
            item_key = item
        if not cache.has_key(item_key):
            cache[item_key] = item
    return cache.values()

def label_count(text):
    ''' calculate count of such labels: Chinese characters, English words, number and punctuations
    '''
    if not isinstance(text, unicode):
        return len(text)
    else:
        count = 0
        fragments = text.split()
        for fragment in fragments:
            has_charactor_list = False
            for uchar in fragment:
                if (uchar >= u'\u0030' and uchar<=u'\u0039') \
                    or (uchar >= u'\u0041' and uchar<=u'\u005a') \
                    or (uchar >= u'\u0061' and uchar<=u'\u007a'):
                    has_charactor_list = True
                else:
                    if has_charactor_list:
                        count += 1
                        has_charactor_list = False
                    count += 1
            if has_charactor_list:
                count += 1
        return count

def append_dict(first, second):
    for key, value in second.items():
        first[key] = value

def separate_dict(source, fields):
    first = {}
    second = {}
    for key, value in source.items():
        if key in fields:
            second[key] = value
        else:
            first[key] = value
    return first, second

def override_dict(source, target, keys = None, not_null = False):
    if keys is None:
        keys = source.keys()
    for key in keys:
        value = source[key]
        if not not_null or value is not None:
            target[key] = value

def dumps_jsonx(dict_obj):
    appends = []
    for key, value in dict_obj.items():
        if key.endswith("__"):
            if not isinstance(value, str):
                raise Exception("append fields must be str, %s" % key)
            dict_obj[key] = [len(appends), len(value)]
            appends.append(value)

    main = simplejson.dumps(dict_obj)
    if len(main) > 0xffffffff:
        raise Exception("dumps_jsonx length exceeded 0xffffffff")

    prefix = "%08x" % (len(main))
    return ''.join([prefix, main] + appends)

def loads_jsonx(str_obj):
    prefix = str_obj[:8]
    str_obj = str_obj[8:]
    main_length = int(prefix, 16)
    main = str_obj[:main_length]
    str_obj = str_obj[main_length:]
    main_obj = simplejson.loads(main)
    appends = sorted(filter(lambda pair : pair[0].endswith("__"), main_obj.items()), key = lambda pair: pair[1][0])
    for i in range(len(appends)):
        key, value = appends[i]
        length = value[1]
        content = str_obj[:length]
        str_obj = str_obj[length:]
        main_obj[key] = content

    return main_obj

def subset(first, second):
    return not find_list(lambda item :  not (item in second), first)

def parse_url(url):
    try:
        return urlparse.urlparse(url)
    except:
        return None

def postprocess(async_mode, result, func):
    if async_mode:
        return result.addBoth(func)
    else:
        return func(result)

def exception_to_str(exception):
    try:
        return str(exception)
    except:
        return ""

if __name__ == "__main__":
    print eval(sys.argv[1])
