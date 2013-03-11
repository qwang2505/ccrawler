# -*- coding: utf-8 -*-

'''
Created on Feb, 22th, 2013

@author dhcui
'''

#import os
import re
import urlparse
#import sys

from ccrawler.policies.policy_interfaces import IUrlAnalyser
from ccrawler.utils.log import logging
import ccrawler.utils.urlnorm as urlnorm
import ccrawler.utils.misc as misc
import ccrawler.utils.tldextract as tldextract
import ccrawler.db.crawlerdb as crawlerdb

domain_url_filenames = ["default.","index."]

ip_regex = re.compile("^\d+\.\d+\.\d+\.\d+$") #Note: the regex is not perfectly precise

scheme_speller_patterns = {
    "http://" : [re.compile("^hhttp://"), re.compile("^htp://"), re.compile("^hthttp://"),re.compile("^hppt://"), re.compile("^httphttp://"),
                 re.compile("^http:// http://"), re.compile("^ttp://"), re.compile("^htthttp://"), re.compile("^http://http://"),
                 re.compile("^http://http://http://"), re.compile("^thttp://"), re.compile("^http://http//"), re.compile("^tp://"),
                 re.compile("^http://http:"), re.compile("^http://thttp://"), re.compile("^http//"), re.compile("^hhtp://"), re.compile("^http:/(?!/)")],
}

chinese_punctuation_map = (
    [u'·', u'～', u'！', u'＠', u'＃', u'￥', u'％', u'……', u'＆', u'×', u'（', u'）', u'－', u'＝', u'【', u'】', u'＼', u'；',
        u'‘', u'’', u'，', u'。', u'、', u'　', u'——', u'＋', u'｛', u'｝', u'｜', u'：', u'“', u'”', u'《', u'》', u'？'],
    "`~!@#$%^&*()-=[]\\;'',./ _+{}|:\"\"<>?",
)

class DefaultUrlAnalyser(IUrlAnalyser):
    '''
    settings fields: mobile_url_patterns, general_crawl_policies.max_url_length/supported_schemes/url_match_domain_type
    db dependent;
    Changes from crawl_url_helper: removed  is_exist_in_dynamic_dict, handle_external_url,
    '''

    def is_mobile_url(self, url):
        for pattern in self._settings["mobile_url_patterns"]:
            if pattern.match(url):
                return True

        for row in crawlerdb.get_mobile_url_patterns():
            url_pattern = row["regex"]
            if isinstance(url_pattern, str) or isinstance(url_pattern, unicode):
                url_pattern = re.compile(url_pattern)
                row["regex"] = url_pattern

            if url_pattern.match(url):
                return True

        return False

    def is_domain_url(self, url):
        parse_result = urlparse.urlparse(url)
        if (len(parse_result.path) == 0 or parse_result.path == "/" or \
            misc.find_list(lambda filename : parse_result.path.startswith(filename),
                domain_url_filenames)) and len(parse_result.query) == 0:
            return True
        else:
            return False

    def is_external_url(self, url, parent_url):
        source_info = misc.get_url_domain_info(url)
        target_info = misc.get_url_domain_info(parent_url)
        return not self.match_url_domain_info(source_info, target_info)

    def normalize_url(self, url, base_url = None):
        if url is None or len(url) == 0:
            return None

        original_url = url
        #Note: here asume all non-unicode urls are encoded by utf-8
        if isinstance(url, str):
            url = url.decode("utf-8")

        if not isinstance(url, unicode):
            logging.error("invalid normalized url, url is not unicode", url = original_url, base_url = base_url)
            return None

        url = url.replace('%20', ' ').strip()

        #fix http scheme:
        url = self._fix_http_scheme(url)

        #handle relative url
        if base_url is not None:
            url = urlparse.urljoin(base_url, url)

        #common normlization
        try:
            url = urlnorm.norm(url)
        except Exception as e:
            logging.warn("invalid normalized url, urlnorm raised exception", url = original_url, base_url = base_url, exception = e)
            return None

        try:
            parse_result = urlparse.urlparse(url)
        except Exception as e:
            logging.warn("invalid normalized url, when parsing url", url = original_url, base_url = base_url)
            return None

        if not parse_result.scheme.lower() in self._settings["general_crawl_policies"]["supported_schemes"]:
            logging.warn("invalid normalized url, not supported schemes", url = original_url, base_url = base_url)
            return None


        netloc = parse_result.netloc
        host = parse_result.netloc.split(':')[0]
        if ip_regex.match(host) is None: #if it's an ip host

            #check if domain and tld exists
            subdomain, domain, tld = tldextract.extract(host)
            if len(domain) == 0 or len(tld) == 0:
                logging.warn("invalid normalized url, no domain or tld", url = original_url, base_url = base_url)
                return None

            #fix chinese punctuation
            for i in range(len(chinese_punctuation_map[0])):
                src = chinese_punctuation_map[0][i]
                dst = chinese_punctuation_map[1][i]
                netloc = netloc.replace(src, dst)

            #add www if not exists
            if len(subdomain) == 0:
                netloc = "www." + netloc

        fragment = parse_result.fragment
        if not fragment.startswith("!"): #Google's recommendation for ajax request
            fragment = ""
        if len(parse_result.scheme) == 0 or len(netloc) == 0:
            logging.warn("invalid normalized url, scheme or netloc is none", url = original_url, base_url = base_url)
            return None

        url = urlparse.urlunparse((parse_result.scheme, netloc, parse_result.path, parse_result.params, parse_result.query, fragment))

        #canonicalize url
        #Note: it's too strong, and sometimes change the url semantics.
        #url = ccrawler.utils.url.canonicalize_url(url)

        url = url.strip()
        if len(url) > self._settings["general_crawl_policies"]["max_url_length"]:
            logging.warn("invalid normalized url, length exceeded", url = original_url, base_url = base_url)
            return None
        elif len(url) == 0:
            logging.warn("invalid normalized url, length too short", url = original_url, base_url = base_url)
            return None
        else:
            return url

    def _fix_http_scheme(self, url):
        for scheme, patterns in scheme_speller_patterns.items():
            for pattern in patterns:
                match = pattern.search(url.lower())
                if match is not None:
                    replaced = match.group(0)
                    url = scheme + url[len(replaced):]
                    return url
        return url

    def match_url_domain_info(self, source_info, target_info):
        source_domain = self.get_url_domain(source_info)
        target_domain = self.get_url_domain(target_info)
        return source_domain == target_domain

    def get_url_domain(self, domain_info):
        domain_type = self._settings["general_crawl_policies"]["url_match_domain_type"]
        if domain_type == "domain":
            return domain_info[0]
        elif domain_type == "full_domain":
            return domain_info[1]
        elif domain_type == "host":
            return domain_info[2]
        else:
            raise Exception("not supported domain_type %s" % domain_type)

    def get_crawl_domain_info(self, url):
        domain_type = self._settings["general_crawl_policies"]["url_match_domain_type"]
        domain_info = misc.get_url_domain_info(url)
        domain = self.get_url_domain(domain_info)
        domain_info = crawlerdb.get_crawl_domain_info(domain, domain_type)
        return domain_info

    def get_url_type(self, url):
        subdomain, _, _ = tldextract.extract(url)
        if self.is_domain_url(url):
            if len(subdomain) == 0 or subdomain == "www":
                return "domain"
            else:
                return "subdomain"
        else:
            return "others"
