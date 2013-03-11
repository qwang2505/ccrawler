import unittest

from ccrawler.cache.domain_decoding_cache import DomainDecodingCache

class DomainDecodingCacheTest(unittest.TestCase):
    def test_main(self):
        domain = "abc.com"
        result = {"utf-8" : 5, "gbk" : 6, "xxx" : 1}
        for decoding, count in result.items():
            for _ in range(count):
                DomainDecodingCache.inc_domain_decoding(domain, decoding)

        decoding = DomainDecodingCache.get_domain_decoding(domain)
        self.assertEqual(decoding, "gbk")


if __name__ == "__main__":
    unittest.main()

