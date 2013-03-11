'''
Created on Jul 9, 2012

@author: dhcui
'''
import cProfile
import datetime
import unittest

import ccrawler.handler.crawl_handler as crawl_handler
import ccrawler.handler.handler
import ccrawler.common.settings as common_settings
import ccrawler.db.crawlerdb as crawlerdb
import ccrawler.utils.misc as misc

def crawl_handler_perf_test():
    file_path = "normalize_urls.tsv"
    urls = misc.load_file(file_path)
    for url in urls:
        message = {
            "url" : url, "source" : "parsed", "crawl_priority" : 1,
            "root_url" : "http://www.baidu.com/", "parent_url" : "http://www.baidu.com/",
            "crawl_depth" : 2,
        }

        ccrawler.handler.handler.HandlerRepository.process("crawl_request", message)

class CrawlHandlerTest(unittest.TestCase):

    test_set = [
        {"message" :
            {"url" : "http://www.sina.com", "source" : "offline", "root_url" : None, "parent_url" : None,
            "crawl_priority" : None, "crawl_depth" : None,},
        "expected_url_info" : {"root_url" : "http://www.sina.com", "parent_url" : None, "crawl_priority" : 1, "crawl_depth" : 1},
        "new_msg" : True, },
        {"message" :
            {"url" : "http://news.sina.com.cn", "source" : "offline", "root_url" : "http://abc", "parent_url" : "http://xyz",
            "crawl_priority" : 3, "crawl_depth" : None,},
        "expected_url_info" : {"root_url" : "http://abc", "parent_url" : "http://xyz", "crawl_priority" : 3, "crawl_depth" : 1},
        "new_msg" : True, },
        {"message" :
            {"url" : "http://news.sina.com.cn/a/b", "source" : "online", "root_url" : "http://abc", "parent_url" : "http://xyz",
            "crawl_priority" : None, "crawl_depth" : None,},
        "expected_url_info" : {"root_url" : "http://abc", "parent_url" : "http://xyz", "crawl_priority" : 0, "crawl_depth" : 0},
        "new_msg" : True, },
        {"message" :
            {"url" : "http://news.sina.com.cn/a/b/c", "source" : "online", "root_url" : None, "parent_url" : None,
            "crawl_priority" : 3, "crawl_depth" : 8,},
        "expected_url_info" : {"root_url" : "http://news.sina.com.cn/a/b/c", "parent_url" : None, "crawl_priority" : 3, "crawl_depth" : 8},
        "new_msg" : True, },
        {"message" :
            {"url" : "http://news.google.com", "source" : "parsed", "root_url" : None, "parent_url" : "http://www.google.com",
            "crawl_priority" : 2, "crawl_depth" : 8,},
        "expected_url_info" : {"root_url" : None, "parent_url" : "http://www.google.com", "crawl_priority" : 3, "crawl_depth" : 7},
        "new_msg" : True, },
        {"message" :
            {"url" : "http://news.google.com/x", "source" : "parsed", "root_url" : None, "parent_url" : "http://www.google.com",
            "crawl_priority" : 3, "crawl_depth" : 8,},
        "expected_url_info" : {"root_url" : None, "parent_url" : "http://www.google.com", "crawl_priority" : 3, "crawl_depth" : 7},
        "new_msg" : True, },
        {"message" :
            {"url" : "http://news.google.cn/x", "source" : "parsed", "root_url" : None, "parent_url" : "http://www.google.com",
            "crawl_priority" : 1, "crawl_depth" : 8,},
        "expected_url_info" : {"root_url" : None, "parent_url" : "http://www.google.com", "crawl_priority" : 2, "crawl_depth" : 7},
        "new_msg" : True, },
        {"message" :
            {"url" : "http://news.google.cn/y", "source" : "parsed", "root_url" : None, "parent_url" : "http://www.google.com",
            "crawl_priority" : 1, "crawl_depth" : 8,},
        "expected_url_info" : {"root_url" : None, "parent_url" : "http://www.google.com", "crawl_priority" : 3, "crawl_depth" : 0},
        "new_msg" : True, "external_crawl_mode" : "new"},
        {"message" :
            {"url" : "http://news.google.com/x", "source" : "parsed", "root_url" : "http://xxx", "parent_url" : "http://www.google.com",
            "crawl_priority" : 2, "crawl_depth" : 5,},
        "expected_url_info" : {"root_url" : None, "parent_url" : "http://www.google.com", "crawl_priority" : 3, "crawl_depth" : 7,
                               "discovered_count" : 2},
        "new_msg" : False, },
        {"message" :
            {"url" : "http://news.google.com/x", "source" : "parsed", "root_url" : "http://xxx", "parent_url" : "http://www.google.com",
            "crawl_priority" : 2, "crawl_depth" : 10,},
        "expected_url_info" : {"root_url" : "http://xxx", "parent_url" : "http://www.google.com", "crawl_priority" : 3, "crawl_depth" : 9,
                               "discovered_count" : 3},
        "new_msg" : False, },
    ]

    mock_message_output = None

    @classmethod
    def process(cls, message_key, message = None, **kw):
        CrawlHandlerTest.mock_message_output = message

    def _test(self):
        crawlerdb.config("localhost", database="test_db")
        crawlerdb.db.urlRepository.drop()
        ccrawler.handler.handler.HandlerRepository.process = CrawlHandlerTest.process

        handler = crawl_handler.CrawlHandler()
        for i in range(len(CrawlHandlerTest.test_set)):
            test_data = CrawlHandlerTest.test_set[i]
            url = test_data["message"]["url"]
            common_settings.general_crawl_policies["external_crawl_mode"] = test_data.get("external_crawl_mode", "continue")
            handler._process(test_data["message"])

            expected_url_info = test_data["expected_url_info"]
            expected_url_info["url"] = url
            expected_url_info["source"] = test_data["message"]["source"]
            expected_url_info["url_class"] = None
            expected_url_info["error_message"] = None
            expected_url_info["crawled_count"] = 0
            expected_url_info["last_crawled"] = None
            expected_url_info["crawl_status"] = "crawling"
            expected_url_info["page_last_modified"] = None
            expected_url_info["original_url"] = None
            if not expected_url_info.has_key("discovered_count"):
                expected_url_info["discovered_count"] = 1

            url_info = crawlerdb.db.urlRepository.find_one({"url" : url})
            self.assertTrue(url_info is not None)
            self.assertTrue(url_info["last_discovered"] is not None)
            self.assertTrue(url_info["created_time"] is not None)
            for field, value in expected_url_info.items():
                self.assertTrue(value == url_info[field], "%s,%s,%s" % (url, field, test_data))
            self.assertTrue(test_data["new_msg"] == (CrawlHandlerTest.mock_message_output != None))
            if test_data["new_msg"]:
                msg = CrawlHandlerTest.mock_message_output
                CrawlHandlerTest.mock_message_output = None
                self.assertTrue(msg["url"] == url)
                self.assertTrue(msg["page_last_modified"] == expected_url_info["page_last_modified"])
                self.assertTrue(msg["__priority"] == expected_url_info["crawl_priority"])

        crawlerdb.db.urlRepository.drop()

    def test_perf(self):
        print datetime.datetime.utcnow()
        cProfile.run("crawl_handler_perf_test()")
        print datetime.datetime.utcnow()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
