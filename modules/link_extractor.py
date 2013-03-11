'''
Created on Feb, 20th, 2013

@author: dhcui
'''

#import datetime

from ccrawler.modules.dom_tree_helper import DomTreeHelper

#import ccrawler.handler.handler as handler
from ccrawler.policies.objects import url_analyser
#from ccrawler.utils.format import datetime2timestamp
from ccrawler.utils import page_parser
from ccrawler.utils.log import logging

class LinkExtractor(object):
    '''
    extract urls
    input: url, doc
    output: success, links, [error_type/message]
    msg sent: crawl_request(url, source, crawl_priority, crawl_depth, root_url, parent_url),
    settings fields: general_crawl_policies.crawl_in_details

    Changes from transcode_processor:
    1) removed html shrink, spam removal, dom tree transcoding, pagination, saves to db;
    2) removed processor de-dup, whether this version page has been handled already;
    3) removed last_finished, last_processed, processed_count, process_status fields.
    4) changed db update for url_class, error_type/message, valid_link_count to be msg output

    '''

    def __init__(self, settings):
        self._settings = settings

    def extract(self, url, html):
        output_msg = {"success" : False, "links" : None}

        #load dom tree
        dom = self._load_doc(url, html)
        if dom is None:
            output_msg["error_type"] = "doc_error"
            output_msg["error_message"] = "raw doc can't be parsed"
            return output_msg

        if not DomTreeHelper.is_valid_dom_tree(dom):
            output_msg["error_type"] = "doc_error"
            output_msg["error_message"] = "dom tree is invalid"
            return output_msg

        #extract links
        link_infos = DomTreeHelper.get_link_infos(dom)
        output_msg["links"] = self._normalize_links(url, link_infos)

        output_msg["success"] = True
        return output_msg

    def _load_doc(self, url, html):
        try:
            dom = page_parser.parse_unicode(html, url, notify=logging.info)
        except:
            logging.error("raw doc can not be parsed", url)
            return None
        return dom

    def _normalize_links(self, url, link_infos):
        links = []
        for link, _ in link_infos:
            link = url_analyser.normalize_url(link, url)
            if link is not None and link != url:
                links.append(link)

        return links
