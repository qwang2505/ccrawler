'''
Created on Jul 4, 2012

@author: dhcui
'''

import HTMLParser
from lxml.html import HtmlElement
import lxml.html

import ccrawler.utils.misc as misc

class DomTreeTraverser(object):
    class Continue(object):
        pass
    class Break(object):
        pass

    def __init__(self, dom, func=None, initial=None):
        self._dom = dom
        self._handlers = []
        if func is not None:
            self.add_handler(func, initial)

    def add_handler(self, func, initial=None):
        self._handlers.append([func, initial])

    def traverse(self):
        self._traverse(self._dom)
        if len(self._handlers) == 0:
            return None
        elif len(self._handlers) == 1:
            return self._handlers[0][1]
        else:
            return map(lambda handler : handler[1], self._handlers)

    def _traverse(self, dom):
        continue_count = 0
        break_count = 0
        for i in range(len(self._handlers)):
            func = self._handlers[i][0]
            result = self._handlers[i][1]
            result = func(dom, result)#Note: take care if result is not returned, it may not be changed.
            if result == DomTreeTraverser.Continue:
                continue_count += 1
            elif result == DomTreeTraverser.Break:
                break_count += 1
            else:
                self._handlers[i][1] = result

        if break_count == len(self._handlers):
            return DomTreeTraverser.Break
        elif continue_count == len(self._handlers):
            return

        children = dom.getchildren()

        for child in children:
            result = self._traverse(child)
            if result == DomTreeTraverser.Break:
                return DomTreeTraverser.Break

class DomTreeHelper(object):

    @classmethod
    def get_node_text(cls, node):
        if isinstance(node, HtmlElement):
            return node.text_content()
        elif isinstance(node, str) or isinstance(node, unicode):
            return node
        else:
            raise Exception("can't get node text")

    @classmethod
    def get_node_html(cls, root_node):
        return lxml.html.tostring(root_node)

    @classmethod
    def get_node_inner_html(cls, root_node):
        if isinstance(root_node, HtmlElement):
            html = root_node.text
            for child in root_node.getchildren():
                html += lxml.html.tostring(child)
            return HTMLParser.HTMLParser().unescape(html)
        elif isinstance(root_node, str) or isinstance(root_node, unicode):
            return root_node
        else:
            raise Exception("can't get node inner_html")

    @classmethod
    def get_anchor_text(cls, node):
        node_text = DomTreeHelper.get_node_text(node)
        if len(node_text) == 0:
            title = node.get("title")
            if title is None:
                return ""
            else:
                return title
        else:
            return node_text

    @classmethod
    def get_link_infos(cls, dom):
        link_nodes = dom.xpath("//a")
        link_infos = []
        for node in link_nodes:
            link = node.get("href")
            text = DomTreeHelper.get_anchor_text(node)
            link_infos.append((link, text))

        return link_infos

    filtered_preprocess_tags = ["script", "link", "noscript", "style"]
    filtered_leaf_tags = ["br"]
    filtered_classids = ["sidebar", "copyright"] # potential: right, left, ads
    filtered_styles = ["display:none", "display: none"]

    @classmethod
    def get_leaf_count(cls, dom):
        def _get_leaf_count(dom, result):
            if not isinstance(dom, HtmlElement):
                return DomTreeTraverser.Continue

            if len(dom.getchildren()) == 0:
                if dom.tag not in DomTreeHelper.filtered_leaf_tags and dom.text is not None and len(dom.text) > 0:
                    result += 1

            return result

        traverser = DomTreeTraverser(dom, _get_leaf_count, 0)
        return traverser.traverse()

    @classmethod
    def preprocess(cls, dom):
        children = dom.getchildren()

        # cut useless/spam dom nodes
        for child in children:
            if not isinstance(child, HtmlElement) or \
                child.tag in DomTreeHelper.filtered_preprocess_tags or \
                dom.get("style") is not None and misc.find_list(dom.get("style"), DomTreeHelper.filtered_styles) or \
                child.get("class") is not None and misc.find_list(child.get("class"), DomTreeHelper.filtered_classids) or \
                child.get("id") is not None and misc.find_list(child.get("id"), DomTreeHelper.filtered_classids):
                child.drop_tree()
            else:
                DomTreeHelper.preprocess(child)

    @classmethod
    def get_tag_count(cls, dom, tag):
        return len(dom.xpath("//" + tag))

    @classmethod
    def get_large_text_count(cls, node, threshold):
        texts = node.xpath(".//text()")
        return len(filter(lambda text : len(text) >= threshold, texts))

    @classmethod
    def get_first_node_html(cls, dom, xpath):
        doms = dom.xpath(xpath)
        if len(doms) > 0:
            return DomTreeHelper.get_node_html(doms[0])
        else:
            return None

    @classmethod
    def is_valid_dom_tree(cls, root):
        return root is not None \
             and root.xpath('//head|//x:head', namespaces={'x':lxml.html.XHTML_NAMESPACE}) \
             and root.xpath('//body|//x:body', namespaces={'x':lxml.html.XHTML_NAMESPACE})
