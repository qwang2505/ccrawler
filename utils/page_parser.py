import re
from BeautifulSoup import UnicodeDammit
from HTMLParser import HTMLParseError
import lxml.html as html

__all__ = [
    'Unparseable',
    'parse',
    'get_title',
    'get_body',
    'ascii']

def debug(s): pass

class Unparseable(ValueError):
    pass

def parse_unicode(content, base_href=None, notify=lambda x: None):
    try:
        cleaned = _remove_crufty_html(content)
        debug("Cleaned content: %s" % (cleaned,))
        return html.fromstring(cleaned)
    except HTMLParseError, e:
        notify("parsing failed: %s" % e)
    raise Unparseable()

def parse(raw_content, base_href=None, notify=lambda x: None):
    try:
        content = UnicodeDammit(raw_content, isHTML=True).markup
#        content = raw_content
        cleaned = _remove_crufty_html(content)
        debug("Cleaned content: %s" % (cleaned,))
        return create_doc(cleaned, base_href)
    except HTMLParseError, e:
        notify("parsing failed: %s" % e)
    raise Unparseable()


class Replacement(object):
    def __init__(self, desc, regex, replacement):
        self.desc = desc
        self.regex = regex
        self.replacement = replacement

    def apply(self, content):
#        # useful for debugging:
#        try:
#            print self. desc + ':' + str(self.regex.findall(content))
#        except RuntimeError: pass
        return self.regex.sub(self.replacement, content)

def create_doc(content, base_href):
    html_doc = html.fromstring(content)
    if base_href:
        html_doc.make_links_absolute(base_href, resolve_base_href=True)
    else:
        html_doc.resolve_base_href()
    return html_doc


# a bunch of regexes to hack around lousy html
dodgy_regexes = (
    Replacement('javascript',
        regex=re.compile('<script.*?</script[^>]*>', re.DOTALL | re.IGNORECASE),
        replacement=''),

    Replacement('double double-quoted attributes',
        regex=re.compile('(="[^"]+")"+'),
        replacement='\\1'),

    Replacement('unclosed tags',
        regex=re.compile('(<[a-zA-Z]+[^>]*)(<[a-zA-Z]+[^<>]*>)'),
        replacement='\\1>\\2'),

    #TODO: fix incorrect html. this regex doesn't work for some encoding reason
    Replacement('incorret html',
        regex=re.compile('(<html)(.*)(/>)(.*<head)', re.I),
        replacement='\\1\\2>\\4'),

    Replacement('unclosed (numerical) attribute values',
        regex=re.compile('(<[^>]*[a-zA-Z]+\s*=\s*"[0-9]+)( [a-zA-Z]+="\w+"|/?>)'),
        replacement='\\1"\\2'),
    )

def _remove_crufty_html(content):
    for replacement in dodgy_regexes:
        content = replacement.apply(content)
    return content


# strip out a set of nuisance html attributes that can mess up rendering in RSS feeds
bad_attrs = ['width', 'height', 'style', '[-a-z]*color', 'background[-a-z]*']
single_quoted = "'[^']+'"
double_quoted = '"[^"]+"'
non_space = '[^ "\'>]+'
htmlstrip = re.compile("<" # open
    "([^>]+) " # prefix
    "(?:%s) *" % ('|'.join(bad_attrs),) + # undesirable attributes
    '= *(?:%s|%s|%s)' % (non_space, single_quoted, double_quoted) + # value
    "([^>]*)"  # postfix
    ">"        # end
, re.I)
def clean_attributes(html):
    while htmlstrip.search(html):
        html = htmlstrip.sub('<\\1\\2>', html)
    return html

