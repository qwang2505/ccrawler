import unittest
import datetime

import ccrawler.common.settings as common_settings
from ccrawler.utils.format import datetime2timestamp
import ccrawler.utils.misc as misc

class RedisClientTest(unittest.TestCase):
    def test_json(self):
        common_settings.redis_cache_config["validation_enabled"] = False
        common_settings.redis_cache_config["data_types"]["json_type"] = {"content_type" : "text/json"}
        common_settings.load_cache_client(True)
        client = common_settings.cache_client()
        data = {"a" : 1, "b" : "xyz"}
        self.assertTrue(client.set("json_type", "abc", data=data))
        self.assertEqual(data, client.get("json_type", "abc"))
        self.assertEqual(None, client.get("json_type", "xyz"))
        data_new = {"a" : 2, "b" : "xyz"}
        self.assertEqual(data, client.set("json_type", "abc", with_get=True, data=data_new))
        self.assertEqual(data_new, client.get("json_type", "abc"))
        client.delete("json_type", "abc")
        self.assertEqual(None, client.get("json_type", "abc"))
        self.assertEqual(True, client.set("json_type", "abc", data=data, nx=True))
        self.assertEqual(False, client.set("json_type", "abc", data=data, nx=True))

    def test_plain(self):
        common_settings.redis_cache_config["validation_enabled"] = False
        common_settings.redis_cache_config["data_types"]["binary_type"] = {"content_type" : "text/plain"}
        common_settings.load_cache_client(True)
        client = common_settings.cache_client()
        data = "xyz"
        self.assertTrue(client.set("binary_type", "abc", data=data))
        self.assertEqual(data, client.get("binary_type", "abc"))
        data_new = "xyz1"
        self.assertEqual(data, client.set("binary_type", "abc", data=data_new, with_get=True))
        self.assertEqual(data_new, client.get("binary_type", "abc"))
        client.delete("binary_type", "abc")
        self.assertEqual(None, client.get("binary_type", "abc"))
        self.assertEqual(True, client.set("binary_type", "abc", data=data_new, nx=True))
        self.assertEqual(False, client.set("binary_type", "abc", data=data_new, nx=True))

    def _test_fail(self, func):
        try:
            func(None)
            self.assertTrue(False)
        except Exception as e:
            if e == AssertionError:
                raise

    def test_hash(self):
        common_settings.redis_cache_config["validation_enabled"] = False
        common_settings.redis_cache_config["data_types"]["hash_type"] = {"content_type" : "redis/hash", "fields" : [("a", int), "b", "c", ("e", long), "d", ("f", lambda v : v == "True", lambda v : "True" if v else "False")]}
        common_settings.load_cache_client(True)
        client = common_settings.cache_client()
        client.delete("hash_type", "abc")
        data = {"a" : 1, "b" : "xyz"}
        self.assertTrue(client.set("hash_type", "abc", update_map=data))
        self.assertEqual({"b" : "xyz", "a" : 1}, client.get("hash_type", "abc", fields = ["b", "a"]))
        self.assertEqual({"b" : "xyz"}, client.get("hash_type", "abc", fields = ["b"]))
        self.assertTrue(client.set("hash_type", "abc", update_map={"c" : "True", "b" : "\\None", "d" : "\\\\None"}))
        self.assertEqual({"a" : 1, "c" : "True", "b": "\\None", "d" : "\\\\None"}, client.get("hash_type", "abc", fields = ["a", "c", "b", "d"]))
        self.assertTrue(client.set("hash_type", "abc", update_map={"b" : None, "f" : 1}, inc_map={"a" : 1, "e" : 2}))
        self.assertEqual({"f" : True, "b" : None, "a" : 2, "e" : 2}, client.get("hash_type", "abc", fields = ["b", "a", "e", "f"]))
        self.assertEqual({"a" : 2}, client.set("hash_type", "abc", inc_map={"a" : 3}, with_get=True, fields=["a"]))
        self.assertEqual({"a" : 5}, client.set("hash_type", "abc", inc_map={"a" : 3}, with_get=True, fields=["a"]))
        self.assertEqual({"a" : 8}, client.get("hash_type", "abc", fields=["a"]))
        client.delete("hash_type", "abc", "b")
        self.assertEqual({"a" : 8, "b" : None}, client.get("hash_type", "abc", fields = ["a", "b"], strict=False))
        self.assertEqual(None, client.get("hash_type", "abc", fields=["a", "not_existed"], strict=True))

        self._test_fail(lambda _ : client.set("hash_type", "abc", inc_map = {"c" : 1}))

        self.assertEqual({"a" : 8}, client.set("hash_type", "abc", inc_map = {"a" : 1}, with_get=True, fields=["a"], cond={"fields" : ["f"], "func" : lambda r : r["f"]}))
        self.assertEqual({"a" : 9}, client.get("hash_type", "abc", fields=["a"]))

        self.assertEqual(False, client.set("hash_type", "abc", inc_map = {"a" : 1}, with_get=True, fields=["a"], cond={"fields" : ["f"], "func" : lambda r : not r["f"]}))
        self.assertEqual({"a" : 9}, client.get("hash_type", "abc", fields=["a"]))

        self.assertEqual(False, client.set("hash_type", "abc", inc_map = {"a" : 1}, with_get=False, fields=["a"], cond={"fields" : ["f"], "func" : lambda r : not r["f"]}))
        self.assertEqual(True, client.set("hash_type", "abc", inc_map = {"a" : 1}, with_get=False, fields=["a"], cond={"fields" : ["f"], "func" : lambda r : r["f"]}))
        self.assertEqual({"a" : 10}, client.get("hash_type", "abc", fields=["a"]))

    def test_set(self):
        common_settings.redis_cache_config["validation_enabled"] = False
        common_settings.redis_cache_config["data_types"]["data_type"] = {"content_type" : "redis/set"}
        common_settings.load_cache_client(True)
        client = common_settings.cache_client()
        client.delete("data_type", None)

        self.assertFalse(client.set("data_type", "first", with_get=True))
        self.assertTrue(client.set("data_type", "first", with_get=True))
        self.assertTrue(client.set("data_type", "first", with_get=False))
        self.assertEqual(True, client.get("data_type", "first"))
        self.assertEqual(False, client.get("data_type", "second"))
        client.delete("data_type", "first")
        self.assertEqual(False, client.get("data_type", "first"))

    def test_not_existed_type(self):
        common_settings.redis_cache_config["validation_enabled"] = False
        common_settings.load_cache_client(True)
        client = common_settings.cache_client()
        data = {"a" : 1, "b" : "xyz"}
        self._test_fail(lambda _ : client.set("binaryxx", "abc", data=data))
        self._test_fail(lambda _ : client.get("binaryxx", "abc"))

    def test_validation(self):
        common_settings.redis_cache_config["validation_enabled"] = True
        common_settings.redis_cache_config["data_types"]["json_type"] = {"content_type" : "text/json"}
        common_settings.load_cache_client(True)
        client = common_settings.cache_client()
        data = {"a" : 1, "b" : "xyz"}
        client.set("json_type", "abc", data=data)
        self.assertEqual(data, client.get("json_type", "abc"))

    def test_url_type(self):
        common_settings.redis_cache_config["validation_enabled"] = False
        common_settings.redis_cache_config["data_types"]["url_test"] = common_settings.redis_cache_config["data_types"]["url"]
        client = common_settings.cache_client()
        url_info = {"crawl_status" : "crawling", "url_class" : None, "crawl_priority" : 1, "crawl_depth" : 0, "last_crawled" : datetime2timestamp(datetime.datetime.utcnow())}
        url = "http://www.baidu.com"
        client.set("url_test", url, update_map = url_info)
        self.assertEqual(url_info, client.get("url_test", url, fields = ["crawl_priority", "crawl_status", "last_crawled", "crawl_depth", "url_class"]))

        url_info = {"crawl_status" : "alive", "url_class" : "details", "crawl_priority" : 3, "crawl_depth" : -1, "last_crawled" : None}
        client.set("url_test", url, update_map = url_info)
        self.assertEqual(url_info, client.get("url_test", url, fields = ["crawl_priority", "crawl_status", "last_crawled", "crawl_depth", "url_class"]))

        client.set("url_test", url, update_map = {"crawl_priority" : 5, "crawl_status" : "notAlive", "last_crawled" : 123})
        self.assertEqual({"crawl_priority" : 5, "crawl_status" : "notAlive", "last_crawled" : 123, "crawl_depth" : -1}, client.get("url_test", url, fields = ["crawl_priority", "crawl_status", "last_crawled", "crawl_depth"]))

    def test_url_dedup_type(self):
        common_settings.redis_cache_config["validation_enabled"] = False
        common_settings.redis_cache_config["data_types"]["url_dedup_test"] = {"content_type" : "redis/set"}
        client = common_settings.cache_client()
        client.delete("url_dedup_test", None)
        url_list = ["http://www.baidu.com", "http://www.google.com", "http://www.sina.com.cn"]
        for url in url_list:
            md5 = misc.md5(url)
            client.set("url_dedup_test", md5)

        for url in url_list:
            self.assertEqual(True, client.get("url_dedup_test", misc.md5(url)))

        self.assertEqual(False, client.get("url_dedup_test", misc.md5("http://www.google.com/")))
        self.assertFalse(client.set("url_dedup_test", misc.md5("http://www.google.com/"), with_get=True))
        self.assertEqual(True, client.get("url_dedup_test", misc.md5("http://www.google.com/")))

    def test_raw_mode(self):
        common_settings.redis_cache_config["validation_enabled"] = False
        data_type = "data_type"
        data_key = "data_key"
        common_settings.redis_cache_config["data_types"][data_type] = {"content_type" : "redis/hash", "raw" : True}
        client = common_settings.cache_client()
        client.delete(data_type, data_key)

        client.set(data_type, data_key, update_map = {"a" : "x"}, inc_map = {"b" : 1})
        self.assertEqual({"a" : "x", "b" : '1'}, client.get(data_type, data_key))
        client.set(data_type, data_key, inc_map = {"c" : 2})
        self.assertEqual({"a" : "x", "b" : '1', "c" : '2'}, client.get(data_type, data_key))

if __name__ == "__main__":
    unittest.main()

