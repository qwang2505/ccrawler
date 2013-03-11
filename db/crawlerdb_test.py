import crawlerdb
import unittest

class CrawlerDbTest(unittest.TestCase):
    def test_get_domain_infos(self):
        crawlerdb.db.crawlDomainWhitelist.drop()

        test_cases = ["http://www.sina.com.cn", "http://www.google.jp"]
        for url in test_cases:
            crawlerdb.save_crawl_domain_info(url)

        domains = map(lambda domain_info: domain_info["domain"], crawlerdb.get_crawl_domain_infos())
        self.assertEqual(domains, ["sina.com.cn", "google.jp"])

        domains = map(lambda domain_info: domain_info["domain"], crawlerdb.get_crawl_domain_infos())
        self.assertEqual(domains, ["sina.com.cn", "google.jp"])

        domains = map(lambda domain_info: domain_info["domain"], crawlerdb.get_crawl_domain_infos())
        self.assertEqual(domains, ["sina.com.cn", "google.jp"])

        domain_info = crawlerdb.get_crawl_domain_info(domain = "sina.com.cn")
        self.assertTrue(domain_info["domain"] == "sina.com.cn")

    def test_get_negative_domains(self):
        crawlerdb.db.crawlDomainBlacklist.drop()
        test_cases = ["sina.com.cn", "google.com"]
        for url in test_cases:
            crawlerdb.save_negative_domain(url)

        domains = crawlerdb.get_negative_domains()
        self.assertEqual(domains, ["sina.com.cn", "google.com"])

        domains = crawlerdb.get_negative_domains()
        self.assertEqual(domains, ["sina.com.cn", "google.com"])

    def test_get_mobile_url_patterns(self):
        crawlerdb.db.mobileUrlPatterns.drop()
        test_cases = ["xyz", "abc"]
        for url in test_cases:
            crawlerdb.save_mobile_url_pattern(url)

        patterns = map(lambda pattern:pattern["regex"], crawlerdb.get_mobile_url_patterns())
        self.assertEqual(patterns, ["xyz", "abc"])

        patterns = map(lambda pattern:pattern["regex"], crawlerdb.get_mobile_url_patterns())
        self.assertEqual(patterns, ["xyz", "abc"])

if __name__ == "__main__":
    unittest.main()
