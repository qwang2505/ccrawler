'''
Created on June, 27, 2012

@author: dhcui
'''

import multiprocessing
import errno
import signal
import sys

#from ccrawler.utils.log import logging
#import ccrawler.common.settings as common_settings
import handler as handler_module
#import ccrawler.utils.misc as misc

logging = None
global_stop_event = multiprocessing.Event()

def stop_condition():
    global_stop_event.is_set()

def config(mqclient_obj, mq_settings_obj, logging_obj, heart_beat_config_obj):
    global logging
    logging = logging_obj
    handler_module.config(mqclient_obj, mq_settings_obj, logging_obj, stop_condition, heart_beat_config_obj)

class HostedHandlerManager(object):
    global_handler_processes = []

    @classmethod
    def stop(cls, signum, frame):
        global_stop_event.set()
        print "all handlers are terminating gracefully"
        logging.debug("all handlers are terminating gracefully")

    @classmethod
    def register_handler(cls, handler_name, handler_type, concurrency):
        handler_class = handler_module._load_object(handler_type)

        for _ in range(concurrency):
            handler_key = HostedHandlerManager._get_unique_handler_key(handler_name)
            handler = handler_class(handler_key = handler_key)

            p = multiprocessing.Process(target=handler.run_loop)
            p.handler = handler
            #p._Popen = MyPopen
            HostedHandlerManager.global_handler_processes.append(p)

    @classmethod
    def _get_unique_handler_key(cls, handler_name):
        existing_handler_keys = map(lambda process : process.handler._handler_key, HostedHandlerManager.global_handler_processes)
        existing_duplicate_handlers = filter(lambda name : name.startswith(handler_name), existing_handler_keys)
        handler_ids = map(lambda name : int(name.split("_")[-1]), existing_duplicate_handlers)
        if len(handler_ids) == 0:
            new_handler_id = 0
        else:
            new_handler_id = max(handler_ids) + 1

        handler_key = handler_name + "_" + str(new_handler_id)
        return handler_key

    @classmethod
    def start(cls):
        for p in HostedHandlerManager.global_handler_processes:
            p.start()

        for p in HostedHandlerManager.global_handler_processes:
            try:
                p.join()
            except OSError, ose:
                if ose.errno != errno.EINTR and ose.errno != errno.ECHILD:
                    raise ose

        logging.debug("all handlers terminated")
