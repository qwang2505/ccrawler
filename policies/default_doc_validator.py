'''
Created on Feb, 22th, 2013

@author dhcui
'''

import ccrawler.utils.misc as misc
from ccrawler.policies.policy_interfaces import IDocValidator

class DefaultDocValidator(IDocValidator):
    '''
    check if the doc is valid, returns True if it's valid
    settings fields: general_crawl_policies.supported_content_types
    '''

    def validate(self, url, html, headers, extras = None):
        #content type filtering
        content_type = headers.get('Content-Type', None)
        if content_type is not None and not misc.find_list(content_type.lower(), self._settings["general_crawl_policies"]["supported_content_types"]):
            False, "filtered by content_type %s" % content_type

        #doc check
        if headers.has_key('Content-Length') and headers['Content-Length'].strip() == "0" or len(html) == 0:
            False,  "doc is empty"

        return True, None
