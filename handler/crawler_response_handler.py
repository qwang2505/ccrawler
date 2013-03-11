'''
Created on June, 27, 2012

@author: dhcui
'''

import re
import httplib
import datetime

from ccrawler.modules.link_extractor import LinkExtractor
from ccrawler.policies.objects import url_validator, url_analyser, recrawl_predictor, doc_validator
from ccrawler.utils.log import logging
import ccrawler.modules.page_analysis as pa
import ccrawler.handler.handler as handler
import ccrawler.db.crawlerdb as crawlerdb
import ccrawler.db.diagnosticsdb as diagdb
import ccrawler.utils.decoder as decoder
import ccrawler.utils.misc as misc
import ccrawler.common.settings as common_settings
from ccrawler.cache.domain_decoding_cache import DomainDecodingCache
from ccrawler.utils.format import datetime2timestamp, timestamp2datetime

class CrawlerResponseHandler(handler.MessageHandler):
    '''
    required message fields:
        created in crawler:   url, original_url, status, doc, headers, page_last_modified, last_crawled, error_message
        url_info meta fields: defined in common_settings.crawler_msg_meta_fields
    db updated fields: error_type/message, encoding/encoding_created_time, doc, headers, first_modified, last_modified/last_crawled, crawled/modified_count, url_class, valid_link_count, recrawl_time/duration/priority, retry/redirect_count, crawl_status, page_last_modified; md5
    sent msg: crawl_request
    '''

    '''
    original redirect handling: if redirected url is a valid url, mark original url as failed, save the redirected url as a new url to db; if the redirected url existes, abandon this crawl result; if it's not a valid url, mark orignal url as failed as well

    current redirect handling: if redirected url is valid, mark original url as alive, save redirect_url to it; create another crawler_response to handle the redirected url.

    decoding logic: if it's dynamic crawling, use utf-8, if encoding in message doesn't expire, use that, else try to find encoding from domain caching, and then decode, and update message encoding field
    md5 logic: update the latest md5 to db, if it's new, set first_modified, if it's changed, set last_modified, and do sth, if it's duplicate, do nothing.
    '''

    '''
    if ok:
        if not redirected: save docs to db, send processor msg if doc changed, update page_last_modified/last_crawled/crawl_status/crawled_count/error_message
        elif it's not valid_crawl_url: update original url's last_crawled/crawl_status/crawled_count/error_message
        else: update original url's crawl_status/last_crawled/crawled_count/error_message, save to urlRedirects
            if actual url exists, return
            else: save docs to db, send processor msg if doc changed, update page_last_modified/last_crawled/crawl_status/crawled_count
    elif not_modified: update last_crawled/crawl_status/crawled_count/error_message
    else: update last_crawled/crawl_status/crawled_count/error_message
    '''

    def initialize(self):
        self._settings = common_settings.core_settings
        self._link_extractor = LinkExtractor(self._settings)

    def _process(self, message):
        update_map = {}
        inc_map = {}

        misc.copy_dict(message["meta"], message, common_settings.crawler_msg_meta_fields)
        url = message["url"]
        if url != message["original_url"]:
            self._handle_redirect(url, message)

        #decode some message fields
        #message updated fields: headers, page_last_modified
        self._decode_fields(url, message)

        #init some message fields
        message["crawl_status"] = "alive"
        self._merge_error_message("crawl_error", message.get("error_message", None), update_map)
        #update_map["redirect_url"] = None

        #main process
        #required message fields: status, original_url, doc, headers, encoding/encoding_created_time, crawl_type, full_domain
        #update_map updated fields: encoding/encoding_created_time, doc, headers, first_modified, last_modified, error_type/message; modified_count
        #message updated fields: crawl_status, doc, first_modified, last_modified, modified_count
        #db updated fields: md5
        message["crawl_status"], md5_hash, error_type, error_message  = self._process_main(url, message, update_map)
        #logging.debug("crawler_response process_main", crawl_status = message["crawl_status"], md5_hash = md5_hash, error_message = error_message)
        misc.copy_dict(update_map, message, ["doc", "first_modified", "last_modified"], soft = True)
        self._merge_error_message(error_type, error_message, update_map)
        if md5_hash is not None:
            inc_map["modified_count"] = 1
            message["modified_count"] += 1

        #page analysis for alive and changed doc
        #required message fields: url_class, doc, crawl_depth, crawl_priority, root_url
        #update_map updated fields: url_class, valid_link_count,  error_type/message
        #message updated fields: url_class, crawl_status
        if md5_hash is not None and message["crawl_status"] == "alive":
            self._handle_modified_doc(url, message, update_map)
        #logging.debug("crawler_response page analysis", crawl_status = message["crawl_status"], error_message = update_map.get("error_message", None))

        #predict recrawl
        #required message fields: url, crawl_status, last_crawled, last_modified, first_modified, modified_count, url_class, crawl_priority, retry/redirect_count
        #update_map updated fields: recrawl_time, recrawl_duration, recrawl_priority, error_type/message, retry/redirect_count
        #message updated fields: crawl_status
        need_retry = False
        need_redirect = False
        if message["crawl_status"] in ["alive", "error", "redirected"]:
            result = recrawl_predictor.predict(url, message)
            misc.copy_dict(result, update_map, ["recrawl_time", "recrawl_duration", "recrawl_priority"], soft = True)
            self._merge_error_message(result.get("error_type", None), result.get("error_message", None), update_map)
            message["crawl_status"] = result["crawl_status"]
            if result["retry_count_inc"]:
                inc_map["retry_count"] = 1
                need_retry = True
            if result["redirect_count_inc"]:
                inc_map["redirect_count"] = 1
                need_redirect = True
        #logging.debug("crawler_response recrawl predictor", crawl_status = message["crawl_status"], error_message = update_map.get("error_message", None))

        #finalize some update_map fields
        #update_map updated fields: last_crawled, crawl_status, crawled_count, page_last_modified, retry/redirect_count
        if not message.has_key("redirect_url"):
            message["redirect_url"] = None
        misc.copy_dict(message, update_map, ["last_crawled", "crawl_status", "redirect_url"])
        inc_map["crawled_count"] = 1
        if message["crawl_status"] == "alive":
            update_map["page_last_modified"] = message["page_last_modified"]
        if not need_retry:
            update_map["retry_count"] = 0
        if not need_redirect:
            update_map["redirect_count"] = 0

        #diagnostics
        diagdb.add_inc(crawled_count = 1, modified_count = 1 if md5_hash is not None else 0)
        if update_map.has_key("error_message") and update_map["error_message"] is not None and len(update_map["error_message"]) > 0:
            logging.warn("crawler_response abnormal", url = url, message = update_map["error_message"])
        else:
            logging.debug("crawler_response finished", url = url)

        #update db
        crawlerdb.update_url_info_by_status(url, "crawling", update_map, inc_map)
        return message

    def _decode_fields(self, url, message):
        #decode some message fields
        if message["page_last_modified"] is not None:
            message["page_last_modified"] = decoder.decode_string(message["page_last_modified"])
            if message["page_last_modified"] is None:
                logging.warn("decode page_last_modified failed", url = url)

        if message["headers"] is not None:
            decoded_headers = {}
            for key in message["headers"].keys():
                value = message["headers"].get(key, "")
                decoded_key = decoder.decode_string(key)
                if decoded_key is None:
                    logging.warn("decoded http response header key failed", url = url, field = unicode({"key" : key, "value" : value}))
                    continue
                if not re.match("^[a-zA-Z0-9-]+$", decoded_key):
                    logging.warn("filtered invalid http response header key", url = url, field = unicode({"key" : key, "value" : value}))
                    continue

                decoded_value = decoder.decode_string(value)
                if decoded_value is None:
                    logging.warn("decoded http response header value failed", url = url, field = unicode({"key" : key, "value" : value}))
                    continue
                decoded_headers[decoded_key] = decoded_value
            message["headers"] = decoded_headers

    def _process_main(self, url, message, update_map):
        #message["md5"] = None
        if message["status"] == httplib.OK:
            success, md5_hash, error_message = self._handle_doc(url, message, update_map)
            return "alive" if success else "failed", md5_hash, "doc_error", error_message
        elif message["status"]  == httplib.NOT_MODIFIED:
            return "alive", None, None, None
        elif message["status"] == 801:
            return "redirected", None, None, None
        elif message["status"] == 802:
            return "failed", None, "redirect_error", "redirect failed"
        else:
            return "error", None, "crawl_error", "http status:%s" % message["status"]

    def _handle_doc(self, url, message, update_map):
        '''
        returns is_valid_doc, changed_doc, error_message, update_map may be changed accordingly.
        '''

        now = datetime2timestamp(datetime.datetime.utcnow())

        success, error_message = doc_validator.validate(url, message["doc"], message["headers"])
        if not success:
            return False, None, error_message

        #decoument decoding
        decoded_doc, used_encoding = self._decode_doc(url, message)
        if decoded_doc is None:
            return False, None, "doc_can't be decoded"
        elif used_encoding != message["encoding"]:
            update_map["encoding"] = used_encoding
            update_map["encoding_created_time"] = now

        #double check crawl type
        #if common_settings.dynamic_crawl_policies["double_check_dynamic_crawl_type"] and \
        #    message["crawl_type"] == "dynamic":
        #    if message["actual_crawl_type"] is not None and message["actual_crawl_type"] != message["crawl_type"]:
        #        update_map["crawl_type"] = message["actual_crawl_type"]

        # content level de-deup by md5 hash
        md5_hash = misc.md5(message["doc"])
        ret = crawlerdb.find_and_modify_url_info_md5(url, md5_hash)

        #notify processor
        if ret != 0: #processor request will not be sent if body has no update.
            if ret == 2:
                update_map["first_modified"] = now
            update_map["doc"] = decoded_doc
            update_map["headers"] = message["headers"]
            update_map["last_modified"] = now
            #message["md5"] = md5_hash
            return True, md5_hash, None
        #elif url != message["original_url"]:
        #    self._handle_abnormal(url, update_map, "failed", "unexpected internal error: redirected new doc can't be inserted")

        return True, None, None

    def _decode_doc(self, url, message):
        if message["crawl_type"] == "dynamic":
            encoding = "utf-8"
        elif message["encoding"] is not None and message["encoding_created_time"] is not None and \
            datetime.datetime.utcnow() - timestamp2datetime(message["encoding_created_time"]) < \
            datetime.timedelta(seconds = self._settings["encoding_expiry_duration"]):
            encoding = message["encoding"]
        else:
            encoding = None

        if encoding is None:
            encoding = DomainDecodingCache.get_domain_decoding(message["full_domain"])

        content_type = message["headers"].get('Content-Type', None)
        decoded_doc, used_encoding = decoder.decode(url, {'Content-Type' : content_type}, \
            message["doc"], encoding)

        return decoded_doc, used_encoding

    def _handle_redirect(self, url, message):
        # TODO how to handle redirect?
        original_url = message["original_url"]

        #Note: double check if the whole flow is consistent
        #add redirected url_info by crawl_handler
        crawl_request_msg = {"url" : url, "source" : "redirected", "parent_url" : original_url, "root_url" : url, "crawl_priority" : message["crawl_priority"], "crawl_depth" : message["crawl_depth"]}
        result = handler.HandlerRepository.process("crawl_request", crawl_request_msg, force_inproc = True)
        if result["status"] >= 0:
            logging.debug(self._log_formatter("redirected succeeded", url = url, original_url = original_url))
            #handle redirected url crawler_response
            crawler_response_msg = misc.clone_dict(message, ["url", "status", "doc", "headers", "page_last_modified", "last_crawled", "error_message"])
            crawler_response_msg["original_url"] = url
            url_info = crawlerdb.get_url_info(url, common_settings.crawler_msg_meta_fields)
            crawler_response_msg["meta"] = url_info
            result = handler.HandlerRepository.process("crawler_response", crawler_response_msg)

            #handle original url crawler_response
            message["url"] = original_url
            message["redirect_url"] = url
            message["status"] = 801
        else:
            message["url"] = original_url
            message["status"] = 802

    def _merge_error_message(self, error_type, error_message, update_map):
        '''
        update_map updated fields: error_message, error_type
        '''

        if error_message is not None and len(error_message) > 0:
            if update_map.get("error_message", None) is not None and len(update_map["error_message"]) > 0:
                update_map["error_message"] += "; " + error_message
            else:
                update_map["error_message"] = error_message

            if error_type is None:
                error_type = "crawl_error"

            if update_map.get("error_type", None) is not None and len(update_map["error_type"]) > 0:
                update_map["error_type"] += "; " + error_type
            else:
                update_map["error_type"] = error_type

    def _handle_modified_doc(self, url, message, update_map):
        html = message["doc"]
        #get url classification
        if message["url_class"] is None:
            success, is_list = pa.is_list_page(url, html)
            if not success:
                logging.error("list page classification failed", url)
            else:
                message["url_class"] = update_map["url_class"] = ("list" if is_list else "details")
        #link extraction depends on crawl_depth, url_class
        if message["crawl_depth"] > 0 and (message["url_class"] == "list" or not self._settings["general_crawl_policies"]["crawl_in_details"]):
            result = self._link_extractor.extract(url, html)
            if result["success"]:
                update_map["valid_link_count"] = len(result["links"])
                for child_url in result["links"]:
                    crawl_request_msg = {
                        "url" : child_url, "source" : "parsed", "crawl_priority" : message["crawl_priority"],
                        "root_url" : message["root_url"], "parent_url" : url,
                        "crawl_depth" : message["crawl_depth"],
                    }

                    handler.HandlerRepository.process("crawl_request", crawl_request_msg)
            else:
                message["crawl_status"] = "failed"
                self._merge_error_message(result["error_type"], result["error_message"], update_map)
