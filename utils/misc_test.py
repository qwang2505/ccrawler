# -*- coding:utf8 -*-
'''
Created on Jul 10, 2012

@author: dhcui
'''

import copy
import unittest
import simplejson
import urllib2

import ccrawler.utils.misc as misc

class MiscTest(unittest.TestCase):
    def test_remove_space(self):
        self.assertTrue(misc.remove_space("abc de\t") == "abcde")
        self.assertTrue(misc.remove_space(u"abc de\t") == u"abcde")
        self.assertTrue(misc.remove_space(u"abc \x0b\u3000de\t") == u"abcde")
        self.assertTrue(misc.remove_space(u"abc \x0b\u3000de\t\t\n\r\xa0\x0b\x0c\u3000x") == u"abcdex")

    def test_label_count(self):
        self.assertTrue(misc.label_count("hello world") == 11)
        self.assertTrue(misc.label_count(u"hello world") == 2)
        self.assertTrue(misc.label_count(u"世界你好") == 4)
        self.assertTrue(misc.label_count(u"hello世界") == 3)
        self.assertTrue(misc.label_count(u"hello world,世界") == 5)

    def test_dumps_jsonx(self):
        test_cases = [{"abc" : 1, "doc__" : "xxyyzz"},{"doc2__" : "xx", "x" : 1, "doc1__" : "yy", "y" : 2}]
        for obj in test_cases:
            print obj
            binary = misc.dumps_jsonx(copy.deepcopy(obj))
            print binary
            ret_obj = misc.loads_jsonx(binary)
            print ret_obj
            self.assertTrue(len(obj) == len(ret_obj))
            for key, value in obj.items():
                self.assertEqual(value, ret_obj[key])
            self.assertTrue(obj == ret_obj)

    def test_dumps_jsonx_body(self):
        req = urllib2.Request("http://www.5173.com")
        res = urllib2.urlopen(req)
        body = res.read()
        body = body.decode("utf-8", "ignore").encode("utf-8")
        message = {"doc__" : body, "url" : "xx"}
        binary = misc.dumps_jsonx(copy.deepcopy(message))
        ret_message = misc.loads_jsonx(binary)
        self.assertEqual(message, ret_message)

        try:
            simplejson.loads(simplejson.dumps(message))
            self.assertFail()
        except:
            pass
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
