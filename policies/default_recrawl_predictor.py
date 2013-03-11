'''
Created on Feb, 22th, 2013

@author dhcui
'''

import datetime

from ccrawler.utils.log import logging
import ccrawler.utils.misc as misc
from ccrawler.utils.format import timestamp2datetime, datetime2timestamp
from ccrawler.policies.policy_interfaces import IRecrawlPredictor
from ccrawler.policies.objects import url_analyser

class DefaultRecrawlPredictor(IRecrawlPredictor):
    '''
    predicts and returns crawl_status, recrawl time, duration, and priority
    '''

    '''
    input: url, crawl_status, last_crawled, last_modified, first_modified, modified_count, url_class, crawl_priority, retry_count, redirect_count
    output: crawl_status, recrawl_time, recrawl_duration, recrawl_priority, retry/redirect_count_inc, [error_type/message]

    settings fields: max_retry_count, retry_wait_interval, total_priority_count, recrawl_policies.url_class.min/max_recrawl_interval/max_alive_interval/mode, max_redirect_count/redirect_wait_interval
    '''

    '''
    min_recrawl_interval < max_recrawl_interval < max_alive_interval
    notAlive condition: last_crawled - last_modified >= max_alive_internal
    triggering condition: now - last_crawled >= min(max(last_crawled - last_modified, min_recrawl_interval, average_modified_period), max_recrawl_interval)
    recrawl policies:
    1) major: predicate whether the document will be updated later or not;
    2) minor: frequently accessed pages can be tried to recrawl;
    '''

    def predict(self, url, url_info, extras = None):
        output_msg = {"crawl_status" : "alive", "recrawl_time" : None, "recrawl_duration" : None, "recrawl_priority" : None, "retry_count_inc" : False, "redirect_count_inc" : False}
        if url_info["url_class"] is None:
            url_info["url_class"] = "undefined"

        if url_info["last_crawled"] is None:
            output_msg["crawl_status"] = "failed"
            output_msg["error_type"] = "unexpected"
            output_msg["error_message"] = "last_crawled is None"
        elif url_info["crawl_status"] == "alive":
            if url_info["modified_count"] <= 0 or url_info["url_class"] is None or url_info["last_modified"] is None or url_info["first_modified"] is None:
                output_msg["crawl_status"] = "failed"
                output_msg["error_type"] = "unexpected"
                output_msg["error_message"] = "any of url_class/last_modified/first_modified is none, or modified_count <= 0: %s" % misc.clone_dict(url_info, ["modified_count", "url_class", "last_modified", "first_modified"])
            else:
                need_recrawl = self._recrawling_url(url, url_info["url_class"])
                if need_recrawl:
                    alive, output_msg["recrawl_time"], output_msg["recrawl_duration"], output_msg["recrawl_priority"] = self._get_recrawl_time_and_priority(url_info)
                    if not alive:
                        output_msg["crawl_status"] = "notAlive"
                else:
                    output_msg["crawl_status"] = "notAlive"
        elif url_info["crawl_status"] == "error":
            if url_info["retry_count"] >= self._settings["recrawl_policies"]["max_retry_count"]:
                output_msg["crawl_status"] = "failed"
                output_msg["error_type"] = "crawl_error"
                output_msg["error_message"] = "retry count exceeded %d" % self._settings["recrawl_policies"]["max_retry_count"]
            else:
                output_msg["recrawl_time"], output_msg["recrawl_duration"], output_msg["recrawl_priority"] = self._get_retry_time_and_priority(url_info)
                output_msg["retry_count_inc"] = True
        elif url_info["crawl_status"] == "redirected":
            if url_info["redirect_count"] >= self._settings["recrawl_policies"]["max_redirect_count"]:
                output_msg["crawl_status"] = "notAlive"
            else:
                output_msg["recrawl_time"], output_msg["recrawl_duration"], output_msg["recrawl_priority"] = self._get_redirect_time_and_priority(url_info)
                output_msg["redirect_count_inc"] = True
        else:
            logging.error("unexpected crawl status", url = url, crawl_status = url_info["crawl_status"])
            output_msg["crawl_status"] = "failed"
            output_msg["error_type"] = "unexpected"
            output_msg["error_message"] = "unexpected crawl status in recrawl:%s" % url_info["crawl_status"]

        if output_msg["recrawl_time"] is not None:
            output_msg["recrawl_time"] = datetime2timestamp(output_msg["recrawl_time"])

        if output_msg["recrawl_duration"] is not None:
            output_msg["recrawl_duration"] = misc.delta_seconds(output_msg["recrawl_duration"])
        return output_msg

    def _get_recrawl_time_and_priority(self, url_info):
        last_crawled = timestamp2datetime(url_info["last_crawled"])
        last_modified = timestamp2datetime(url_info["last_modified"])
        first_modified = timestamp2datetime(url_info["first_modified"])
        modified_count = url_info["modified_count"]

        #calculate next document modification predication.
        average_modified_period = misc.diff_seconds(last_crawled, first_modified) / modified_count # may be slightly negative
        last_modified_since = misc.diff_seconds(last_crawled, last_modified) #may be slightly negative

        min_recrawl_interval = self._settings["recrawl_policies"]["url_class_policies"][url_info["url_class"]]["min_recrawl_interval"]
        max_recrawl_interval = self._settings["recrawl_policies"]["url_class_policies"][url_info["url_class"]]["max_recrawl_interval"]
        max_alive_interval   = self._settings["recrawl_policies"]["url_class_policies"][url_info["url_class"]]["max_alive_interval"]

        if last_modified_since >= max_alive_interval:
            return False, None, None

        last_modified_since = min(max(last_modified_since, min_recrawl_interval, average_modified_period), max_recrawl_interval)

        recrawl_duration = datetime.timedelta(seconds = last_modified_since)

        #calculate recrawl priority, recrawl priority is in [crawl_priority, Lowest_priority] range.
        crawl_priority = url_info["crawl_priority"]
        delta = (last_modified_since - min_recrawl_interval) * 1.0 / (max_recrawl_interval - min_recrawl_interval)
        recrawl_priority = int(crawl_priority + (self._settings["total_priority_count"] - crawl_priority - 1) * delta)

        return True, last_crawled + recrawl_duration, recrawl_duration, recrawl_priority

    def _get_retry_time_and_priority(self, url_info):
        last_crawled = timestamp2datetime(url_info["last_crawled"])
        recrawl_duration = datetime.timedelta(seconds = self._settings["recrawl_policies"]["retry_wait_duration"])
        recrawl_priority = url_info["crawl_priority"]
        return last_crawled + recrawl_duration, recrawl_duration, recrawl_priority

    def _get_redirect_time_and_priority(self, url_info):
        last_crawled = timestamp2datetime(url_info["last_crawled"])
        recrawl_duration = datetime.timedelta(seconds = self._settings["recrawl_policies"]["redirect_wait_duration"])
        recrawl_priority = url_info["crawl_priority"]
        return last_crawled + recrawl_duration, recrawl_duration, recrawl_priority

    def _recrawling_url(self, url, url_class):
        mode = self._settings["recrawl_policies"]["url_class_policies"][url_class]["mode"]
        if mode == "none":
            return False
        elif mode == "all":
            return True
        elif mode == "whitelist":
            domain_info = url_analyser.get_crawl_domain_info(url)
            if url_class == "details":
                return domain_info is not None and domain_info["recrawl_details"]
            elif url_class == "list":
                return domain_info is not None and domain_info["recrawl_list"]
            elif url_class == "undefined":
                return domain_info is not None and domain_info["recrawl_undefined"]
            else:
                raise Exception("not supported url class %s" % url_class)
        else:
            raise Exception("not supported recrawling mode %s" % mode)
