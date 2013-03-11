'''
Created on Jul 9, 2012

@author: dhcui
'''
import unittest
import httplib
import hashlib

import ccrawler.handler.handler
import ccrawler.utils.decoder as decoder
import ccrawler.common.settings as common_settings
import ccrawler.db.crawlerdb as crawlerdb
from ccrawler.handler.crawler_response_handler import CrawlerResponseHandler

class CrawlerResponseHandlerTest(unittest.TestCase):

    initial_set = [
            {"url" : "http://www.sina.com", "source" : "offline", "crawl_priority" : 3, "crawl_depth" : 3},
            {"url" : "http://www.sina.com/x", "source" : "offline", "crawl_priority" : 3, "crawl_depth" : 3},
            {"url" : "http://www.baidu.com", "source" : "online", "crawl_priority" : 3, "crawl_depth" : 3},
        ]

    whitelist = ["http://www.sina.com", "http://www.baidu.com", "http://www.google.com"]

    test_set = [
        #first
        {"message" : {"url" : "http://www.sina.com", "original_url" : "http://www.sina.com",
             "page_last_modified" : "xxx", "last_crawled" : "xyz", "status" : httplib.OK, "doc" : "hello world", "headers" : {}},
         "expected_url_info" : {"crawl_status" : "processing", "error_message" : None, "page_last_modified" : "xxx", "last_crawled" : "xyz", "crawled_count" : 1,
             "modified_count" : 1},
         "url_redirects_count" : 0,
         "new_msg" : True,
        }, #0
        #doc changed
        {"message" : {"url" : "http://www.sina.com", "original_url" : "http://www.sina.com",
             "page_last_modified" : "xxx", "last_crawled" : "xyz", "status" : httplib.OK, "doc" : "hello world again", "headers" : {}},
         "expected_url_info" : {"crawl_status" : "processing", "error_message" : None, "page_last_modified" : "xxx", "last_crawled" : "xyz", "crawled_count" : 2,
             "modified_count" : 2},
         "url_redirects_count" : 0,
         "new_msg" : True,
        }, #1
        #doc not changed
        {"message" : {"url" : "http://www.sina.com", "original_url" : "http://www.sina.com",
             "page_last_modified" : "yyy", "last_crawled" : "xxx", "status" : httplib.OK, "doc" : "hello world again", "headers" : {}},
         "expected_url_info" : {"crawl_status" : "alive", "error_message" : None, "page_last_modified" : "yyy", "last_crawled" : "xxx", "crawled_count" : 3,
             "modified_count" : 2},
         "url_redirects_count" : 0,
         "new_msg" : False,
        }, #2
        #duplicate redirect, will be abandoned
        {"message" : {"url" : "http://www.sina.com", "original_url" : "http://www.sina.com/x",
             "page_last_modified" : "xxx", "last_crawled" : "xyz", "status" : httplib.OK, "doc" : "hello world again", "headers" : {}},
         "expected_url_info" : {"url" : "http://www.sina.com/x", "crawl_status" : "redirected", "error_message" : None, "page_last_modified" : None, "last_crawled" : "xyz", "crawled_count" : 1},
         "expected_raw_doc" : None,
         "url_redirects_count" : 1,
         "new_msg" : False,
        }, #3
        #redirected and new changes
        {"message" : {"url" : "http://www.sina.com/y", "original_url" : "http://www.sina.com/x",
             "page_last_modified" : "xxxx", "last_crawled" : "xyz", "status" : httplib.OK, "doc" : "hello world again 2", "headers" : {}},
         "expected_url_info" : {"original_url" : "http://www.sina.com/x", "crawl_status" : "processing", "error_message" : None, "page_last_modified" : "xxxx", "last_crawled" : "xyz", "crawled_count" : 1,
             "modified_count" : 1},
         "url_redirects_count" : 1,
         "new_msg" : True,
        }, #4
        #redirected to non-valid url, not in whitelist
        {"message" : {"url" : "http://www.xxxyyy.com/y", "original_url" : "http://www.sina.com/x",
             "page_last_modified" : "xxx", "last_crawled" : "xyz", "status" : httplib.OK, "doc" : "hello world again 2", "headers" : {}},
         "expected_url_info" : {"url" : "http://www.sina.com/x", "crawl_status" : "redirected_filtered", "error_message" : None, "page_last_modified" : None, "last_crawled" : "xyz", "crawled_count" : 3, "discovered_count" : 1},
         "expected_raw_doc" : None,
         "url_redirects_count" : 1,
         "new_msg" : False,
        }, #5
        #no modified
        {"message" : {"url" : "http://www.baidu.com/x", "original_url" : "http://www.baidu.com",
             "page_last_modified" : "xxx", "last_crawled" : "xyz", "status" : httplib.NOT_MODIFIED, "doc" : "hello world again 2", "headers" : {}},
         "expected_url_info" : {"url" : "http://www.baidu.com", "crawl_status" : "failed", "page_last_modified" : None, "last_crawled" : "xyz", "crawled_count" : 1},
         "expected_raw_doc" : None,
         "url_redirects_count" : 1,
         "new_msg" : False,
        }, #6
        #error
        {"message" : {"url" : "http://www.baidu.com/x", "original_url" : "http://www.baidu.com",
             "page_last_modified" : "xxxxxx", "last_crawled" : "xyz", "status" : httplib.NOT_FOUND, "doc" : "hello world again 2", "headers" : {}},
         "expected_url_info" : {"url" : "http://www.baidu.com", "crawl_status" : "failed", "error_message" : str(httplib.NOT_FOUND), "page_last_modified" : None, "last_crawled" : "xyz", "crawled_count" : 2},
         "expected_raw_doc" : None,
         "url_redirects_count" : 1,
         "new_msg" : False,
        }, #7
        #redirected and new changes and external url
        {"message" : {"url" : "http://www.google.com", "original_url" : "http://www.sina.com/x",
             "page_last_modified" : "xxx", "last_crawled" : "xyz", "status" : httplib.OK, "doc" : "hello world again 2", "headers" : {}},
         "expected_url_info" : {"original_url" : "http://www.sina.com/x", "crawl_status" : "processing", "crawl_priority" : 3, "crawl_depth" : 0, "error_message" : None, "page_last_modified" : "xxx", "last_crawled" : "xyz", "crawled_count" : 1,
              "modified_count" : 1},
         "external_crawl_mode" : "new",
         "url_redirects_count" : 1,
         "new_msg" : True,
        }, #8

    ]

    mock_message_output = None

    def _test_raw_doc(self, expected, message, result):
        expected["url"] = message["url"]
        expected["doc"] = message["doc"]
        expected["headers"] = message["headers"]
        expected["md5"] = hashlib.md5(message["doc"]).hexdigest()
        for field, value in expected.items():
            self.assertTrue(value == result[field], "%s,%s,%s" % (message["url"], field, message))


    @classmethod
    def process(cls, message_key, message = None, **kw):
        if message_key == "processor_request":
            CrawlerResponseHandlerTest.mock_message_output = message

    @classmethod
    def decode(cls, url,headers,body, cache_encoding = True):
        return body, "utf-8"

    class MockMqClient(object):
        def publish(self, key, message, need_serialize = True, persistent = True, content_type = "text/json", priority = -1):
            pass

    @classmethod
    def mqclient(cls):
        return CrawlerResponseHandlerTest.MockMqClient()

    def test(self):
        common_settings.mqclient = CrawlerResponseHandlerTest.mqclient
        ccrawler.handler.handler.HandlerRepository.init({})
        crawlerdb.config("localhost", database="test_db")
        crawlerdb.db.urlRepository.drop()
        crawlerdb.db.rawDocs.drop()
        crawlerdb.db.urlRedirects.drop()
        crawlerdb.db.crawlDomainWhitelist.drop()
        for url in CrawlerResponseHandlerTest.whitelist:
            crawlerdb.add_crawl_domain_whitelist(url)

        for message in CrawlerResponseHandlerTest.initial_set:
            ccrawler.handler.handler.HandlerRepository.process("crawl_request", message)

        decoder.decode = CrawlerResponseHandlerTest.decode
        ccrawler.handler.handler.HandlerRepository.process = CrawlerResponseHandlerTest.process

        handler = CrawlerResponseHandler()
        for i in range(len(CrawlerResponseHandlerTest.test_set)):
            test_data = CrawlerResponseHandlerTest.test_set[i]
            common_settings.general_crawl_policies["external_crawl_mode"] = test_data.get("external_crawl_mode", "continue")
            common_settings.general_crawl_policies["url_match_target"] = "whitelist"
            url = test_data["message"]["url"]
            print i, url
            if test_data["expected_url_info"].has_key("url"):
                url = test_data["expected_url_info"]["url"]

            handler._process(test_data["message"])

            expected_url_info = test_data["expected_url_info"]
            expected_url_info["url"] = url
            #expected_url_info["source"] = test_data["message"]["source"]
            expected_url_info["url_class"] = None
            expected_url_info["parent_url"] = None
            expected_url_info["root_url"] = test_data["message"]["original_url"]
            if not expected_url_info.has_key("original_url"):
                expected_url_info["original_url"] = None
            if not expected_url_info.has_key("crawl_priority"):
                expected_url_info["crawl_priority"] = 3
            if not expected_url_info.has_key("crawl_depth"):
                expected_url_info["crawl_depth"] = 3
            if not expected_url_info.has_key("discovered_count"):
                expected_url_info["discovered_count"] = 1

            if not expected_url_info.has_key("modified_count"):
                expected_url_info["modified_count"] = 0

            url_info = crawlerdb.db.urlRepository.find_one({"url" : url})
            self.assertTrue(url_info is not None)
            self.assertTrue(url_info["last_discovered"] is not None)
            self.assertTrue(url_info["created_time"] is not None)
            if not test_data.has_key("expected_raw_doc"):
                self.assertTrue(url_info["last_modified"] is not None)
                self.assertTrue(url_info["first_modified"] is not None)

            for field, value in expected_url_info.items():
                self.assertTrue(value == url_info[field], "%d,%s,%s,%s" %(i, field, value, url_info[field]))

            raw_doc = crawlerdb.db.rawDocs.find_one({"url" : url})
            test_data["expected_raw_doc"] = test_data.get("expected_raw_doc", {})
            if test_data["expected_raw_doc"] is None:
                self.assertTrue(raw_doc is None)
            else:
                self.assertTrue(raw_doc is not None)
                self._test_raw_doc(test_data["expected_raw_doc"], test_data["message"], raw_doc)

            self.assertTrue(crawlerdb.db.urlRedirects.count() == test_data["url_redirects_count"],
                "%d,%d,%d" % (i, crawlerdb.db.urlRedirects.count(), test_data["url_redirects_count"]))

            self.assertTrue(test_data["new_msg"] == (CrawlerResponseHandlerTest.mock_message_output != None))
            if test_data["new_msg"]:
                msg = CrawlerResponseHandlerTest.mock_message_output
                CrawlerResponseHandlerTest.mock_message_output = None
                self.assertTrue(msg["url"] == url)

            #mock process finished
            crawlerdb.db.urlRepository.update({"url" : url}, {"$set" : {"crawl_status" : "alive"}})

        crawlerdb.db.urlRepository.drop()
        crawlerdb.db.rawDocs.drop()
        crawlerdb.db.urlRedirects.drop()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
