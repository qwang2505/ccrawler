# -*- coding: utf-8 -*-

'''
Created on Jun 30, 2012

@author: dhcui
'''

import unittest
import cProfile
import datetime

import ccrawler.common.settings as common_settings
from ccrawler.common.crawl_url_helper import CrawlUrlHelper
import ccrawler.db.crawlerdb as crawlerdb
import ccrawler.utils.misc as misc

def normalize_url_perf_test():
    file_path = "./normalize_urls.tsv"
    urls = misc.load_file(file_path)
    for url in urls:
        CrawlUrlHelper.normalize_url(url)

def valid_crawl_url_perf_test():
    file_path = "./normalize_urls.tsv"
    urls = misc.load_file(file_path)
    for url in urls:
        CrawlUrlHelper.valid_crawl_url(url, None)

class CrawlUrlHelperTest(unittest.TestCase):
    def test_mobile_url(self):
        test_set = {
            "http://m.baidu.com/search" : True,
            "http://3g.163.com" : True,
            "http://wapp.sina.com.cn" : False,
            "http://weibo.com/a/b" : False,
            "http://www.weibo.cn" : False,
            "https://weibo.cn" : False,
            "http://weibo.cn/a/b/c" : True,
        }

        for url, expected in test_set.items():
            self.assertEqual(CrawlUrlHelper._mobile_url(url), expected)

    def test_match_parent_url(self):
        test_set = [
            ("http://www.google.jp", "http://www.google.com", "domain", True),
            ("http://www.google.jp", "http://www.google.com", "full_domain", False),
            ("http://news.sina.com.cn", "http://www.sina.com.cn", "full_domain", True),
            ("http://news.sina.com.cn", "http://www.sina.com.cn", "host", False),
            ("http://news.sina.com.cn", "http://www.sina.cn/a/b", "domain", True),
            ("http://news.sina.com.cn/a/b", "http://www.sina.cn", "full_domain", False),
            ("http://3g.sina.cn/a/b", "http://www.sina.cn", "domain", False),
        ]

        for url, parent_url, match_section, expected in test_set:
            common_settings.general_crawl_policies["url_match_domain_type"] = match_section
            common_settings.general_crawl_policies["url_match_target"] = "parent_url"
            self.assertEqual(CrawlUrlHelper.valid_crawl_url(url, parent_url), expected)

    def test_match_whitelist(self):
        test_whitelist = [
            "http://www.google.com",
            "http://www.sina.com.cn",
            "http://www.sina.cn",
        ]

        test_set = [
            ("http://www.google.jp", "domain", False),
            ("http://www.google.jp", "full_domain", False),
            ("http://news.sina.com.cn", "full_domain", False),
            ("http://news.sina.com", "full_domain", False),
            ("http://news.sina.com.cn", "host", False),
            ("http://news.sina.cn", "domain", False),
            ("http://news.sina.com.cn/a/b", "host", False),
            ("http://3g.sina.cn/a/b", "domain", False),
        ]

        crawlerdb.config("localhost", database="test_db")
        crawlerdb.db.crawlDomainWhitelist.drop()
        for url in test_whitelist:
            crawlerdb.save_crawl_domain_info(url)

        for url, match_section, expected in test_set:
            common_settings.general_crawl_policies["url_match_domain_type"] = match_section
            common_settings.general_crawl_policies["url_match_target"] = "whitelist"
            self.assertEqual(CrawlUrlHelper.valid_crawl_url(url, None), expected)

        crawlerdb.db.crawlDomainWhitelist.drop()

    def test_get_crawl_priority_and_depth(self):
        test_whitelist = {
            "http://www.google.com" : {},
            "http://www.sina.com.cn" : {"crawl_priority" : 2},
            "http://www.sina.cn" : {"crawl_priority" : 2, "crawl_depth" : 3},
        }

        test_set = [
            ("http://www.google.jp", "offline", 1, 2), #not whitelist, domain, offline
            ("http://news.sina.com.cn", "online", 2, 0), #whitelist, subdomain, online
            ("http://news.sina.com", "online", 0, 0), #not whitelist, subdomain, online
            ("http://news.sina.cn/a/b", "offline", 2, 3), #whitelist, others, offline
        ]

        crawlerdb.config("localhost", database="test_db")
        crawlerdb.db.crawlDomainWhitelist.drop()
        for url, config in test_whitelist.items():
            crawlerdb.save_crawl_domain_info(url, \
                crawl_priority = config.get("crawl_priority", -1), crawl_depth = config.get("crawl_depth", -1))

        for url, source, expected_priority, expected_depth in test_set:
            print url
            priority, depth = CrawlUrlHelper.get_crawl_priority_and_depth(url, source)
            self.assertEqual(priority, expected_priority)
            self.assertEqual(depth, expected_depth)

        crawlerdb.db.crawlDomainWhitelist.drop()

    def test_external_url(self):
        self.assertTrue(not CrawlUrlHelper._external_url("http://www.sina.com.cn", "http://news.sina.com.cn"))
        self.assertTrue(CrawlUrlHelper._external_url("http://www.sina.com.cn", "http://news.sina.cn"))

    def test_valid_crawl_url(self):
        common_settings.general_crawl_policies["url_match_target"] = "all"
        self.assertTrue(CrawlUrlHelper.valid_crawl_url("http://www.pingpinganan.gov.cn/web/index.aspx", "http://www.taobao.com"))
        link = "http://www.pingpinganan.gov.cn/web/index.aspx"
        url = "http://www.taobao.com"
        not_valid = not CrawlUrlHelper._valid_url(link) or not CrawlUrlHelper.valid_crawl_url(link, url) or link == url
        self.assertTrue(not_valid == False)

    def test_valid_crawl_url2(self):
        link = "http://www.cafewoo.com/tags.php?/%D1%DB%D3%B0%B5%C4%BB%AD%B7%A8/"
        url = "http://www.cafewoo.com/yanzhuang/201202/22-1608.html"
        self._test_valid_crawl_url(link, url)

    def _test_valid_crawl_url(self, link, url):
        common_settings.general_crawl_policies["url_match_target"] = "all"
        self.assertTrue(CrawlUrlHelper.valid_crawl_url(link, url))
        not_valid = not CrawlUrlHelper._valid_url(link) or not CrawlUrlHelper.valid_crawl_url(link, url) or link == url
        self.assertTrue(not_valid == False)
        self.assertTrue(CrawlUrlHelper.valid_crawl_url(u"http://www.追女孩子的技巧/", None))

    def test_normalize_url(self):
        url = "http://www.cafewoo.com/tags.php?/%D1%DB%D3%B0%B5%C4%BB%AD%B7%A8/"
        self.assertTrue(CrawlUrlHelper.normalize_url(url))
        url = "http://www.baidu.com/../../a"
        self.assertTrue(CrawlUrlHelper.normalize_url(url))
        url = "abc"
        self.assertFalse(CrawlUrlHelper.normalize_url(url))

        url = "http://weibo.com"
        self.assertTrue(CrawlUrlHelper.normalize_url(url) == "http://www.weibo.com/")

        self.assertTrue(CrawlUrlHelper.normalize_url("http://www.追女孩子的技巧/".decode("utf-8")) is None)

        url = "http://www.baidu.com/#!abc"
        self.assertTrue(CrawlUrlHelper.normalize_url(url) == url)

        url = "http://tr.jumei.com/user/gotoshare/click--www.meilishuo.com/share/share?content=@%E8%81%9A%E7%BE%8E%E4%BC%98%E5%93%81+%E4%BB%8A%E6%97%A5%E7%83%AD%E5%8D%96%E5%8D%95%E5%93%81%EF%BC%9A%E5%9B%BD%E9%99%85%E9%A1%B6%E7%BA%A7%E6%97%B6%E5%B0%9A%E6%9D%82%E5%BF%97%E2%80%9C%E5%87%BA%E9%95%9C%E7%8E%87%E2%80%9D%E6%9C%80%E9%AB%98%E7%9A%84%E4%B8%93%E4%B8%9A%E7%BE%8E%E7%94%B2%E5%93%81%E7%89%8C%EF%BC%81%E4%BB%85%E5%94%AE69%E5%85%83%EF%BC%8COPI%E6%8C%87%E7%94%B2%E6%B2%B9%E7%BA%AF%E9%BB%91%E6%99%B6%E9%92%BB15ml%EF%BC%8C%E5%AE%9B%E8%8B%A5%E9%92%BB%E7%9F%B3%E8%88%AC%E9%97%AA%E8%80%80%EF%BC%8C%E8%AE%A9%E4%BD%A0%E5%9C%A8%E4%B8%BE%E6%89%8B%E6%8A%95%E8%B6%B3%E4%B9%8B%E9%97%B4%E6%98%BE%E9%9C%B2%E5%87%BA%E7%A5%9E%E7%A7%98%E9%85%B7%E6%84%9F%E5%8D%81%E8%B6%B3%E7%9A%84%E6%97%B6%E5%B0%9A%E9%A3%8E%E6%83%85%E3%80%82%E9%97%AA%E7%83%81%E6%97%A0%E6%AF%94%E7%9A%84%E4%BA%AE%E7%89%87%E8%A2%AB%E8%BF%90%E7%94%A8%E5%88%B0%E5%85%B6%E4%B8%AD%EF%BC%8C%E7%BB%BD%E7%8E%B0%E7%B2%BE%E8%87%B4%E9%94%8B%E8%8A%92%E3%80%82%E5%88%9B%E9%80%A0%E5%87%BA%E4%B8%80%E7%A7%8D%E6%97%A0%E4%B8%8E%E4%BC%A6%E6%AF%94%E7%9A%84%E9%92%BB%E7%9F%B3%E7%BA%A7%E7%BE%8E%E7%94%B2%E6%95%88%E6%9E%9C%EF%BC%8C%E6%9B%B4%E5%8A%A0%E5%87%B8%E6%98%BE%E4%BD%A0%E7%9A%84%E6%98%8E%E6%98%9F%E7%89%B9%E8%B4%A8%E3%80%82&url=http://www.jumei.com/%3Fr%3Di0%26referer%3Dshare_meilishuo&image=http://p0.jmstatic.com/deal_product/b1/j9/bj120803p15691/bj120803p15691-sidedeal.jpg"
        self.assertTrue(CrawlUrlHelper.normalize_url(url) is None)

        common_settings.general_crawl_policies["url_match_target"] = "all"

        self.assertTrue(CrawlUrlHelper.valid_crawl_url(url, None))

        self.assertTrue(CrawlUrlHelper.normalize_url("http:/www.baidu.com") == "http://www.baidu.com/")
        self.assertTrue(CrawlUrlHelper.normalize_url("http:/www.baidu.com/a%20/b/%20") == "http://www.baidu.com/a%20/b/")
        self.assertTrue(CrawlUrlHelper.normalize_url("htp://www.baidu.com") == "http://www.baidu.com/")
        self.assertTrue(CrawlUrlHelper.normalize_url("htp://www。a.baidu.com") == "http://www.a.baidu.com/")
        self.assertTrue(CrawlUrlHelper.normalize_url("htp://www。a.baidu.com".decode("utf-8")) == "http://www.a.baidu.com/")
        self.assertTrue(CrawlUrlHelper.normalize_url("http://xn--efvx5o.cn") == "http://www.新浪.cn/".decode("utf-8"))
        self.assertTrue(CrawlUrlHelper.normalize_url("http://203.207.98.252/zzxglj/") is not None)
        self.assertTrue(CrawlUrlHelper.normalize_url("http://WWW.WHNEWS.CN") is not None)
        self.assertTrue(CrawlUrlHelper.normalize_url("http://www.vogue.com.cn%20%20") == "http://www.vogue.com.cn/")

    def test_remove_fragment(self):
        url = "http://www.baidu.com/a/b?q=xx#adsa"
        expected_url = "http://www.baidu.com/a/b?q=xx"
        result = CrawlUrlHelper.normalize_url(url)
        self.assertTrue(result == expected_url)


    def test_normalize_url_perf(self):
        print datetime.datetime.utcnow()
        cProfile.run("normalize_url_perf_test()")
        print datetime.datetime.utcnow()

    def test_valid_crawl_url_perf(self):
        print datetime.datetime.utcnow()
        cProfile.run("valid_crawl_url_perf_test()")
        print datetime.datetime.utcnow()

if __name__ == "__main__":
    unittest.main()
