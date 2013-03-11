'''
Created on Feb, 26th, 2013

@author: dhcui
'''

import ccrawler.utils.misc as misc
import ccrawler.common.settings as common_settings

class CrawlerUtils(object):
    @classmethod
    def build_crawler_request_msg(cls, url, url_info):
        message = misc.clone_dict(url_info, ["url", "page_last_modified"])
        message["__priority"] = url_info["crawl_priority"]
        message["meta"] = misc.clone_dict(url_info, common_settings.crawler_msg_meta_fields)
        if common_settings.strong_politeness:
            message["__group_hash"] = url_info["full_domain"]
        else:
            message["__group_hash"] = misc.md5(url)

        if url_info["crawl_type"] == "static":
            message_type = "crawler_request"
        elif url_info["crawl_type"] == "dynamic":
            message_type = "dynamic_crawler_request"
        else:
            raise Exception("unsupported crawl_type %s" % url_info["crawl_type"])

        return message_type, message
