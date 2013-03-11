'''
Created on 2012-7-18

@author: baiwu
'''
import sys

import ccrawler.static_crawler.settings as settings
import ccrawler.common.settings as common_settings
common_settings.override_settings(settings)
from ccrawler.handler.hosted_handler_manager import HostedHandlerManager

if len(sys.argv) > 1:
    if len(sys.argv) > 2:
        concurrency = sys.argv[2]
    else:
        concurrency = "1"

    common_settings.hosted_handlers = {sys.argv[1] : {"concurrency" : concurrency}}

HostedHandlerManager.launch()
