import unittest
from ccrawler.cache.url_cache_client import UrlCacheClient
import ccrawler.common.settings as common_settings

class UrlCacheClientTest(unittest.TestCase):
    def test_main(self):
        url = ["http://www.xyz.com/xyz", "http://www.xyz.com/xyz"]
        for u in url:
            common_settings.cache_client().delete("url_dedup", u)
            common_settings.cache_client().delete("url", u)

        self.assertEqual(False, common_settings.cache_client().exists("url_dedup", url[0]))
        self.assertEqual((False, True), UrlCacheClient.check_url_exists(url[0]))
        self.assertEqual((True, True), UrlCacheClient.check_url_exists(url[0]))
        update_map = {"crawl_priority" : 1, "crawl_depth" : 2, "crawl_status" : "crawling", "url_class" : None, "last_crawled" : 123, "not_included" : "xyz", "md5" : None}
        self.assertEqual(True, UrlCacheClient.update_url_info(url[0], update_map))
        self.assertEqual({"crawl_priority" : 1, "url_class" : None}, UrlCacheClient.get_url_info(url[0], fields=["crawl_priority", "url_class"]))
        self.assertEqual({"crawl_priority" : 1, "url_class" : None}, UrlCacheClient.get_url_info_by_status(url[0], "crawling", fields=["crawl_priority", "url_class"]))
        self.assertEqual(False, UrlCacheClient.get_url_info_by_status(url[0], "notAlive", fields=["crawl_priority", "url_class"]))
        self.assertEqual(True, UrlCacheClient.update_url_info_by_status(url[0], "crawling", {"crawl_priority" : 2}))
        self.assertEqual(False, UrlCacheClient.update_url_info_by_status(url[0], "alive", {"crawl_priority" : 3}))
        self.assertEqual({"crawl_priority" : 2, "url_class" : None}, UrlCacheClient.find_and_modify_url_info(url[0], {"url_class" : "list"}, {}, ["crawl_priority", "url_class"]))
        self.assertEqual({"crawl_priority" : 2, "url_class" : "list"}, UrlCacheClient.find_and_modify_url_info_by_status(url[0], "crawling", {"crawl_depth" : 3}, {}, ["crawl_priority", "url_class"]))
        self.assertEqual(False, UrlCacheClient.find_and_modify_url_info_by_status(url[0], "notAlive", {"crawl_status" : "alive"}, {}, ["crawl_priority", "url_class"]))
        self.assertEqual({"md5" : None}, UrlCacheClient.find_and_modify_url_info_by_not_md5(url[0], "xyz", {"md5" : "xyz"}, {}, ["md5"]))
        self.assertEqual(False, UrlCacheClient.find_and_modify_url_info_by_not_md5(url[0], "xyz", {"md5" : "xyz"}, {}, ["md5"]))
        self.assertEqual({"crawl_priority" : 2, "crawl_depth" : 3, "crawl_status" : "crawling", "url_class" : "list", "last_crawled" : 123, "md5" : "xyz"}, UrlCacheClient.get_url_info(url[0]))
        self.assertEqual(True, UrlCacheClient.update_url_info(url[0], {"crawl_status" : "notAlive"}))
        self.assertEqual(False, common_settings.cache_client().exists("url", url[0]))
        self.assertEqual("0", common_settings.cache_client().get("url_dedup", url[0]))
        self.assertEqual((True, False), UrlCacheClient.check_url_exists(url[0]))

if __name__ == "__main__":
    unittest.main()
