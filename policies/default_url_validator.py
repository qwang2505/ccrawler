'''
Created on Feb, 22th, 2013

@author dhcui
'''

import urlparse
import os

import ccrawler.utils.misc as misc
import ccrawler.db.crawlerdb as crawlerdb
from ccrawler.policies.policy_interfaces import IUrlValidator
from ccrawler.policies.objects import url_analyser

class DefaultUrlValidator(IUrlValidator):
    '''
    checks if the url is a valid crawl url, returns True if it's valid.
    db dependent;
    settings fields: negative_url_patterns/extensions/domains, general_crawl_policies.url_match_target/url_match_domain_type, domain
    '''

    def validate(self, url, parent_url, extras = None):
        '''
        it's normalized url
        '''

        source_info = misc.get_url_domain_info(url)

        #check whether it's a mobile url
        if url_analyser.is_mobile_url(url):
            return False

        #check whether it's a negative url pattern
        for pattern in self._settings["negative_url_patterns"]:
            if pattern.match(url):
                return False

        #check whether it's a negative url extension
        parse_result = urlparse.urlparse(url)
        ext = os.path.splitext(parse_result.path)[1].lower()
        if len(ext) > 0 and ext in self._settings["negative_url_extensions"]:
            return False

        #check whether it's a negative url domain
        negative_domains = self._settings["negative_url_domains"]
        if source_info[1].lower() in negative_domains:
            return False

        # TODO why read from db? just add dependents. this should not be
        # a default action. If want to, user should implements their own url
        # validator, and replace default one in settings, to do all kind of
        # stuff they want to.
        negative_domains = crawlerdb.get_negative_domains()
        if source_info[1].lower() in negative_domains:
            return False

        #check filtering policy
        # what dose this settings mean? See options in common/configuration.py
        match_target = self._settings["general_crawl_policies"]["url_match_target"]
        if match_target == "none":
            return False
        elif match_target == "whitelist":
            domain = url_analyser.get_url_domain(source_info)
            # what does this settings mean? See options in common/configuration.py
            domain_type = self._settings["general_crawl_policies"]["url_match_domain_type"]
            # TODO read from db again... still think this should be done by
            # user.
            whitelist = crawlerdb.get_crawl_domain_infos(domain_type)
            # if domain in white list, valid
            return len(filter(lambda domain_row : domain_row["domain"] == domain, whitelist)) > 0
        elif match_target == "parent_url":
            if parent_url is None:
                return True
            target_info = misc.get_url_domain_info(parent_url)
            return url_analyser.match_url_domain_info(source_info, target_info)
        elif match_target == "all":
            return True
        else:
            raise Exception("not supported match_target %s" % match_target)
