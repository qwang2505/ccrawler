'''
Created on Feb, 20th, 2013

@author: dhcui
'''

import datetime

from ccrawler.utils.log import logging
import ccrawler.handler.handler as handler
#import ccrawler.utils.misc as misc
import ccrawler.common.settings as common_settings
import ccrawler.db.crawlerdb as crawlerdb
#from ccrawler.utils.format import timestamp2datetime, datetime2timestamp
from ccrawler.modules.crawler_utils import CrawlerUtils

class SortScheduler(handler.TimingHandler):
    '''
    db input: besides necessary crawler_request fields, we need url
    db output: crawl_status, last_crawl_start_time
    msg sent: crawler_request

    Changes from RecrawlScheduler:
    1) no need receive messages from mq;
    2) removed global_pending_url_info_cache(the message need to wait for next crawl time), and global_async_message_cache(the message need to wait crawl_status is expected for recrawl), and then the scheduler is stateless, easy to recover
    3) removed duplicate crawlings detection;
    4) no care about if process_status is successful;

    '''

    def _process(self):
        while True:
            now = datetime.datetime.utcnow()
            url_info = crawlerdb.find_and_modify_expired_url_info(now, common_settings.crawler_msg_meta_fields)
            if url_info is None:
                break

            url = url_info["url"]
            message_type, crawler_request_msg = CrawlerUtils.build_crawler_request_msg(url, url_info)
            handler.HandlerRepository.process(message_type, crawler_request_msg)
            logging.debug(self._log_formatter("sent to crawler", url = url))
