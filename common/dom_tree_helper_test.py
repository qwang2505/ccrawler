import unittest
import datetime

import lxml.html

from ccrawler.common.dom_tree_helper import DomTreeHelper
import ccrawler.utils.misc as misc

class DomTreeHelperTest(unittest.TestCase):
    def test_get_node_text(self):
        dom = lxml.html.fromstring("<a>hello <b>HELLO <s>xyz</s></b> world</a>")
        self.assertEqual(DomTreeHelper.get_node_text(dom), "hello HELLO xyz world")

        dom = lxml.html.fromstring("head <a>hello <b>HELLO <s>xyz</s></b> world</a> tail")
        self.assertEqual(DomTreeHelper.get_node_text(dom), "head hello HELLO xyz world tail")

        dom = lxml.html.fromstring("abc")
        self.assertEqual(DomTreeHelper.get_node_text(dom), "abc")

    def test_get_node_inner_html(self):
        dom = lxml.html.fromstring("<a>hello <b>HELLO <s>xyz</s></b> world</a>")
        self.assertEqual(DomTreeHelper.get_node_inner_html(dom), "hello <b>HELLO <s>xyz</s></b> world")

        dom = lxml.html.fromstring("head <a>hello <b>HELLO <s>xyz</s></b> world</a> tail")
        self.assertEqual(DomTreeHelper.get_node_inner_html(dom), "head <a>hello <b>HELLO <s>xyz</s></b> world</a> tail")

        dom = lxml.html.fromstring("abc")
        self.assertEqual(DomTreeHelper.get_node_inner_html(dom), "abc")

    def test_get_node_html(self):
        dom = lxml.html.fromstring("<a>hello <b>HELLO <s>xyz</s></b> world</a>")
        self.assertEqual(DomTreeHelper.get_node_html(dom), "<a>hello <b>HELLO <s>xyz</s></b> world</a>")

        dom = lxml.html.fromstring("head <a>hello <b>HELLO <s>xyz</s></b> world</a> tail")
        self.assertEqual(DomTreeHelper.get_node_html(dom), "<p>head <a>hello <b>HELLO <s>xyz</s></b> world</a> tail</p>")

        dom = lxml.html.fromstring("abc")
        self.assertEqual(DomTreeHelper.get_node_html(dom), "<p>abc</p>")

    def test_get_anchor_text(self):
        dom = lxml.html.fromstring("<a>hello world <b>xxx</b></a>")
        self.assertEqual(DomTreeHelper.get_anchor_text(dom), "hello world xxx")

        dom = lxml.html.fromstring("<a title='hello'/>")
        self.assertEqual(DomTreeHelper.get_anchor_text(dom), "hello")

        dom = lxml.html.fromstring("<a/>")
        self.assertEqual(DomTreeHelper.get_anchor_text(dom), "")

    def test_get_tag_count(self):
        dom = misc.load_doc("http://www.taobao.com", False)
        start = datetime.datetime.now()
        c = DomTreeHelper.get_tag_count(dom, "a")
        self.assertTrue(c > 500)
        end = datetime.datetime.now()
        print end - start

    def test_get_leaf_count(self):
        dom = misc.load_doc("http://www.taobao.com", True)
        #start = datetime.datetime.now()
        c = DomTreeHelper.get_leaf_count(dom)
        self.assertTrue(c > 500)
        #check1 = datetime.datetime.now()
        #c1 = DomTreeHelper.get_leaf_count2(dom)
        #end = datetime.datetime.now()
        #self.assertTrue(c == c1)
        #print c, c1, check1 - start, end - check1
