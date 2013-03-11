'''
Created on Feb, 22th, 2013

@author dhcui
'''

from ccrawler.utils.log import logging

from ccrawler.policies.policy_interfaces import ICrawlPriorityAndDepthEvaluator
from ccrawler.policies.objects import url_analyser, url_validator

# TODO sumarize strategy used to determine priority and depth
class DefaultCrawlPriorityAndDepthEvaluator(ICrawlPriorityAndDepthEvaluator):
    '''
    determines static crawl priority for new external urls, redirect urls, extracted urls from doc;
    url_info fields: crawl_priority, crawl_depth, parent_url
    returns valid_url, crawl_priority, crawl_depth
    settings fields: general_crawl_policies.domain_based_crawl_priority_and_depth/external_crawl_mode, crawl_policies.source.crawl_priority/crawl_depth.url_type, total_priority_count
    '''

    def evaluate(self, url, source, url_info, extras = None):
        crawl_priority = -1
        crawl_depth = -1
        # if exist in url info, read it
        if url_info["crawl_priority"] is not None:
            crawl_priority = url_info["crawl_priority"]
        if url_info["crawl_depth"] is not None:
            crawl_depth = url_info["crawl_depth"]

        #url validation,
        if not url_validator.validate(url, url_info["parent_url"]):
            logging.warn("invalid crawl url", url = url, parent_url = url_info["parent_url"])
            return False, crawl_priority, crawl_depth

        #for non-parsed urls, determined based on domains or defaults.
        if source == "offline" or source == "online" or source == "post_ondemand":
            if url_info["crawl_priority"] is None or url_info["crawl_depth"] is None:
                # determine priority and depth by souce
                crawl_priority, crawl_depth = self._determine(url, source)
                #use default explicit ones
                if url_info["crawl_priority"] is not None:
                    crawl_priority = url_info["crawl_priority"]
                if url_info["crawl_depth"] is not None:
                    crawl_depth = url_info["crawl_depth"]

        #for parsed urls, priority += 1, depth -= 1
        # TODO why priority + 1?
        elif source == "parsed" or source == "redirected":
            crawl_priority = url_info["crawl_priority"]
            if crawl_priority < self._settings["total_priority_count"] - 1:
                crawl_priority += 1

            crawl_depth = url_info["crawl_depth"] - 1

            #handle external url
            if url_analyser.is_external_url(url, url_info["parent_url"]):
                mode = self._settings["general_crawl_policies"]["external_crawl_mode"]
                # mode could be continue or new. if new, use source determine
                # new priority and depth.
                if mode == "new":
                    crawl_priority, crawl_depth = self._determine(url, "external")
        else:
            raise Exception("unsupported source %s", source = source)

        # raise exceed expetion
        if crawl_priority < 0 or crawl_priority >= self._settings["total_priority_count"]:
            raise Exception("priority exceeded %s" % crawl_priority)

        if crawl_depth < 0:
            raise Exception("crawl_depth can't be less than 0 %s" % crawl_depth)

        return True, crawl_priority, crawl_depth

    def _determine(self, url, source):
        crawl_priority = -1
        crawl_depth = -1
        #use domain based priority/depth
        if self._settings["general_crawl_policies"]["domain_based_crawl_priority_and_depth"]:
            domain_info = url_analyser.get_crawl_domain_info(url)
            if domain_info is not None:
                crawl_priority = domain_info["crawl_priority"]
                crawl_depth = domain_info["crawl_depth"]

        #use default priority/depth, determine by source
        if crawl_priority == -1:
            crawl_priority = self._settings["crawl_policies"][source]["crawl_priority"]

        if crawl_depth == -1:
            # url_type could be domain, subdomain, others.
            url_type = url_analyser.get_url_type(url)
            crawl_depth = self._settings["crawl_policies"][source]["crawl_depth"][url_type]

        return crawl_priority, crawl_depth
