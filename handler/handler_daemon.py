'''
Created on Feb, 27th, 2013

@author: dhcui
'''

import signal
import sys

import ccrawler.common.settings as common_settings
from ccrawler.handler.hosted_handler_manager import HostedHandlerManager, stop_condition

def launch(hosted_handlers):
    signal.signal(signal.SIGTERM, HostedHandlerManager.stop)
    signal.signal(signal.SIGINT, HostedHandlerManager.stop) # for ctrl-c

    common_settings.mqclient().set_stop_condition(stop_condition)
    common_settings.cache_client().set_stop_condition(stop_condition)


    for handler_name, handler_config in hosted_handlers.items():
        handler_type = common_settings.mq_settings["handler_configs"][handler_name]["type"]
        # TODO get settings module from config
        handler_settings = common_settings.mq_settings["handler_configs"][handler_name].get("settings", None)
        if handler_settings is not None:
            handler_settings = __import__(handler_settings, {}, {}, [''])
            common_settings.override_settings(handler_settings)
        concurrency = int(handler_config.get("concurrency", "0"))
        HostedHandlerManager.register_handler(handler_name, handler_type, concurrency)

    HostedHandlerManager.start()

if __name__ == "__main__":
    handler_name = sys.argv[1]
    concurrency = sys.argv[2]
    hosted_handlers = {}
    hosted_handlers[handler_name] = {"concurrency" : concurrency}

    launch(hosted_handlers)
