'''
Created on Sep, 08, 2012

@author: dhcui
'''

import copy
import random
import datetime

import ccrawler.handler.handler as handler
import ccrawler.utils.misc as misc

from ccrawler.utils.format import datetime2timestamp
from ccrawler.utils.log import logging

import ccrawler.static_crawler.settings as settings
import ccrawler.static_crawler.dns_cache as dns_cache
import ccrawler.static_crawler.downloaders as downloaders

class StaticCrawlerHandler(handler.MessageHandler):
    '''
    msg input: url, page_last_modified,
    '''

    """
    internal supported features: timeout, user_agent, default_headers, compression, referer, redirect, DNS cache, 304, http/https, robots.txt, chunked,
    global supported features: retry, caching, encoding, dedup,  filtering, url length, depth, error handling, DOM tree, parsing, link extraction
    TODO P2 features: proxy, authentication, cookies, multimedia, ftp, non-get
    """

    def initialize(self):
        # enable dns cache
        if settings.dns_cache_enabled:
            dns_cache.enable_dns_cache()

        # enable user agent rotate
        if settings.user_agent_rotation_enabled:
            self._user_agent_list = misc.load_user_agent(settings.user_agent_file)
            if len(self._user_agent_list) == 0:
                self._user_agent_list.append(settings.default_user_agent)
                logging.error("user agent can't be downloaded, use default one")
        else:
            self._user_agent_list = [settings.default_user_agent]

        # initialize downloader, use twisted or urllib2
        if settings.downloader_type == "twisted":
            self._downloader = downloaders.TwistedDownloader()
        elif settings.downloader_type == "urllib2":
            self._downloader = downloaders.UrlLib2Downloader()
        else:
            raise Exception("unsupported downloader type")

    def _process(self, message):
        url = message["url"]

        #start crawling
        return self.crawl_url(self._async_mode, url, message, self.get_user_agent(), None, message["page_last_modified"])

    def crawl_url(self, async_mode, url, input_message, user_agent, referer, page_last_modified):
        request_header = copy.deepcopy(settings.default_headers)
        #set user agent
        if user_agent is not None:
            request_header['User-Agent'] = user_agent

        #set referer
        if referer is not None:
            request_header["Referer"] = referer;

        #set if_modified_since
        if page_last_modified is not None:
            request_header['If-Modified-Since'] = page_last_modified

        meta = input_message

        meta["dns_cache_enabled"] = settings.dns_cache_enabled
        meta["chunked_transfer_decoding"] = settings.chunked_transfer_decoding

        #start crawling
        result =self._downloader.crawl(
            async_mode,
            url,
            settings.timeout,
            request_header,
            settings.robotstxt_enabled,
            meta)

        return misc.postprocess(self._async_mode, result, self.process_crawler_response)

    def process_crawler_response(self, result):
        if not result.has_key("url"):
            return None
        if result["status"] == 700:
            self.crawl_url(self._async_mode, result["url"], result["meta"], self.get_user_agent(), None, result["meta"]["page_last_modified"])
            return result["meta"]
        else:
            #send crawler_response message
            input_msg = result["meta"]
            fields = ["url", "status", "doc", "headers"]
            message = misc.clone_dict(result, fields)
            message["page_last_modified"] = input_msg["page_last_modified"]
            message["original_url"] = input_msg["url"]
            message["last_crawled"] = datetime2timestamp(datetime.datetime.utcnow())
            message["error_message"] = result.get("error_message", None)
            message["meta"] = input_msg["meta"]
            message["meta"]["crawl_type"] = "static"
            if result["headers"] is not None and result["headers"].has_key("Last-Modified"):
                message["page_last_modified"] = result["headers"].get('Last-Modified')

            handler.HandlerRepository.process("__internal_crawler_response", message)
            return result["meta"]

    def get_user_agent(self):
        return random.choice(self._user_agent_list)
