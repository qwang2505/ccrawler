#import sys
import unittest
import cProfile

from ccrawler.static_crawler.static_crawler_handler import StaticCrawlerHandler
import ccrawler.common.settings as common_settings
from twisted.internet import reactor

def run():
    handler = StaticCrawlerHandler()
    count = 0
    while count < 50:
        message = common_settings.mqclient().get("crawler_request")
        if message is not None:
            handler._process(message)
        count += 1

def _async_run():
    count = {"count" : 0}
    def callback(result, count):
        count["count"] += 1
        if count["count"] == 50:
            print "stopped"
            reactor.stop()

    handler = StaticCrawlerHandler()
    for _ in range(50):
        dfd = handler._main_process_async(None)
        dfd.addBoth(callback, count)

    reactor.run()


class StaticCrawlerHandlerTest(unittest.TestCase):
    def _test_perf(self):
        cProfile.run("run()")

    def _test_async_perf(self):
        cProfile.run("_async_run()")

    def test_basic(self):
        handler = StaticCrawlerHandler()
        #handler._main()
        #message = common_settings.mqclient().get("crawler_request", wait_secs = -1)
        message = {
            "url" : "http://www.zongheng.com/",
            "url_class" : None,
            "root_url" : None,
            "crawl_priority" : 1,
            "crawl_depth" : 0,
            "full_domain" : "zongheng.com",
            "page_last_modified" : None,
            "__priority" : 1,
            "encoding" : None,
            "encoding_created_time" : None,
            "redirect_url" : None,
        }
        #print message
        dfd = handler._process(message)
        print "waiting"
        def callback(result):
            print "finished crawling", result


        dfd.addBoth(callback)
        reactor.run()

if __name__ == "__main__":
    unittest.main()
