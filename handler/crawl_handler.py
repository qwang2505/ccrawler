'''
Created on June, 27, 2012

@author: dhcui
'''


from ccrawler.utils.log import logging
from ccrawler.policies.objects import url_analyser, crawl_priority_and_depth_evaluator
import ccrawler.handler.handler as handler
#import ccrawler.db.crawlerdb as crawlerdb
from ccrawler.modules.crawler_utils import CrawlerUtils
import ccrawler.utils.misc as misc

class CrawlHandler(handler.MessageHandler):
    '''
    msg input: url, source, parent_url, root_url, [crawl_priority, crawl_depth], others needed by crawler_request
    msg ouput: crawler_request
    db output: new full url_info, or update
    '''

    '''
    update crawl_priority/crawl_depth, adds url_info to db, merge by crawl_depth if duplicate, send msg if new.
    crawl policy:
    1) offline/online request used explicit caller setting or default configuration
    2) parsed request will inherit priority from parent, and then decrease until to tail

    Note: the crawl request url may be need to be redirect if we lookup urlRedirects table,
    no need to do this redirect map to make sure the url-redirect info keep updated, and crawler will handle this
    '''

    def _process(self, message):
        # normalize url
        url = url_analyser.normalize_url(message["url"])
        if url is None:
            logging.error("invalid url for crawl", url = message["url"])
            return {"status" : -1}
        message["url"] = url

        #fill optional fields
        url_info = misc.clone_dict(message, fields = ["url", "source", "root_url", "parent_url", "crawl_priority", "crawl_depth"])

        if url_info["root_url"] is None:
            url_info["root_url"] = url

        #deterimine crawl priority/depth
        is_valid, url_info["crawl_priority"], url_info["crawl_depth"] = crawl_priority_and_depth_evaluator.evaluate(url, url_info["source"], url_info)
        if not is_valid:
            return {"status" : -1}

        # stores to urlRepository table
        url_info["page_last_modified"] = None
        url_info["crawl_status"] = "crawling"
        url_info["last_crawled"] = None
        url_info["original_url"] = None
        # all urls is static now
        url_info["crawl_type"] = "static"
        # TODO add to crawler db, this should not be done here
        # some project do not need to store url info into database
        # should use middleware for these kind of actions
        #success, promoted = crawlerdb.add_url_info(url, url_info, True)

        if message["source"] != "redirected":
            # notify crawler
            message_type, crawler_message = CrawlerUtils.build_crawler_request_msg(url, url_info)
            handler.HandlerRepository.process(message_type, crawler_message)

        return {"status" : 1}
