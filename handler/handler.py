'''
Created on June, 27, 2012

@author: dhcui

Before use any functions in this module, call config(mq_settings, logging) to init
mq_settings required fields: handler_configs, message_configs

Changes log:
2013/02/27: simplication for independent module: disabled heartbeat, copied load_object method, changed mq_settings/logging dependencies to be IoC, disabled async mode;
'''

import time
import sys
import datetime
import inspect
import copy
import traceback

#from twisted.internet import reactor, defer

#from ccrawler.utils.log import logging
#import ccrawler.common.settings as mq_settings
#import ccrawler.handler.heartbeat as heartbeat

logging = None
mq_settings = None
stop_condition = None
mqclient = None
heart_beat_config = None

def config(mqclient_obj, mq_settings_obj, logging_obj, stop_condition_func, heart_beat_config_obj):
    if mqclient_obj is None or mq_settings_obj is None or logging_obj is None or stop_condition_func is None or heart_beat_config_obj is None:
        raise Exception("neither of arguments should be none")

    global mqclient, logging, mq_settings, stop_condition, heart_beat_config
    mqclient = mqclient_obj
    mq_settings = mq_settings_obj
    logging = logging_obj
    stop_condition = stop_condition_func
    heart_beat_config = heart_beat_config_obj

def _load_object(path):
    try:
        dot = path.rindex('.')
    except ValueError:
        raise ValueError, "Error loading object '%s': not a full path" % path

    module, name = path[:dot], path[dot+1:]
    try:
        mod = __import__(module, {}, {}, [''])
    except ImportError, e:
        raise ImportError, "Error loading object '%s': %s" % (path, e)

    try:
        obj = getattr(mod, name)
    except AttributeError:
        raise NameError, "Module '%s' doesn't define any object named '%s'" % (module, name)

    return obj

class HandlerBase(object):
    '''
    _handle_expected_exception: returns Exception, or dict
    _process: returns dict or None
    process: returns None for viaqueue, and returns dict for inproc, will change None to {} for _process
    '''

    def __init__(self, handler_key):
        if handler_key is None:
            self._handler_key = self.__class__.__name__
        else:
            self._handler_key = handler_key

        self.initialize()

    def initialize(self):
        pass

    #can only be called via run_loop
    def _main_process(self):
        pass


    def _before_stop(self):
        pass

    def run_loop(self):
        '''
        run handler in endless loop in one dedicate process
        '''
        logging.info("subprocess for handler started", self._handler_key)
        #to avoid two duplicate handler in subprocess
        if isinstance(self, MessageHandler) and \
        mq_settings["handler_configs"][self.__class__.__name__].get("mode", "viaqueue") == "inproc":
            HandlerRepository.global_inproc_handlers[self.__class__.__name__] = self

        #start heartbeat client thread
        if heart_beat_config["client_enabled"]:
            #self._heart_beat_client = heartbeat.HeartBeatClient(heart_beat_config, self._handler_key, self._stop_condition)
            heart_beat_client_class = _load_object(heart_beat_config["client_class"])
            self._heart_beat_client = heart_beat_client_class(heart_beat_config, self._handler_key, stop_condition)
            self._heart_beat_client.start_by_thread()

        self._main()

        logging.info("loop stopped for handler", self._handler_key)
        self._before_stop()
        sys.exit(0)

    def _main(self):
        while not stop_condition():
            self._main_process()

    def _log_formatter(self, message, *args, **kwargs):
        return logging.format_msg("%s,%s" % (self._handler_key, message), *args, **kwargs)

class MessageHandler(HandlerBase):
    '''
    handler._process can be used in two scenarios:
    1) a separate process to run an endless loop that consumes message from message queue and processes.
    2) ondemand approach to let client side to call the method directly.

    HandlerRepository.process can be used in two modes:
    1) viaqueue mode: will publish message to queue directly;
    2) inproc mode:   will call handler._process;
    '''

    def __init__(self, handler_key = None):
        super(MessageHandler, self).__init__(handler_key)
        handler_config = mq_settings["handler_configs"][self.__class__.__name__]
        self._message_key = handler_config["input_message"]
        self._async_mode = handler_config.get("async_mode", False)
        self._async_concurrency = handler_config.get("async_concurrency", 1)

    def process(self, message):
        if self._async_mode:
            raise Exception("process() just support sync_mode operation")
        #logging.debug(self._log_formatter("start processing message", message.get("url", "")))
        message = copy.deepcopy(message)
        try:
            return self._process(message)
        except Exception as e:
            logging.error("unexpected exception raised!!!", e = e, url = message.get("url", ""))
            callstack = traceback.format_exc()
            logging.error("callstack", callstack)
            result = self._handle_unexpected_exception(message, e, callstack)
            if result is None:
                return {}
            elif isinstance(result, Exception):
                raise result
            else:
                return result
        #logging.debug(self._log_formatter("ended processing message", message.get("url", "")))

    def _process(self, message):
        pass

    def _handle_unexpected_exception(self, message, exception, callstack):
        return Exception("unexpected exception raised in inproc message processing %s\ncallstack: %s" % (exception, callstack))

    def _build_message_from_args(self, frame): # not used now
        message = {}

        args, _, _, values = inspect.getargvalues(frame)
        for arg in args:
            if arg != "self":
                message[arg] = values[arg]

        return message

    def _main_process(self):
        message = mqclient().get(self._message_key, wait_secs=-1)
        if message is not None:
            delivery_tag = message["__delivery_tag"]
            logging.debug(self._log_formatter("start processing message", message.get("url", "")))
            success = self.process(message)
            if success:
                mqclient().ack(self._message_key, delivery_tag)
            logging.debug(self._log_formatter("ended processing message", message.get("url", "")))
        else:
            logging.debug(self._log_formatter("message queue is empty or handler is terminating", self._message_key))

    def reply(self, message, response):
        mqclient().reply(self._message_key, message, response)

    #def _postprocess(self, input_message):
    #    if input_message is not None:
    #        mq_settings.mqclient().ack(self._message_key, input_message["__delivery_tag"])
    #        logging.debug(self._log_formatter("ended processing message", input_message.get("url", "")))
    #    if not self._stop_condition():
    #        dfd = defer.Deferred()
    #        dfd.addBoth(self._main_process_async)
    #        reactor.callLater(0, dfd.callback, None)
    #    return input_message

    #def _main_process_async(self, _):
    #    message = mq_settings.mqclient().get(self._message_key, wait_secs=0)
    #    if message is not None:
    #        logging.debug(self._log_formatter("start processing message", message.get("url", "")))
    #        dfd = self._process(message)
    #        dfd = dfd.addBoth(self._postprocess)
    #    else:
    #        logging.debug(self._log_formatter("message queue is empty or handler is terminating", self._message_key))
    #        dfd = defer.Deferred()
    #        dfd.addBoth(self._main_process_async)
    #        reactor.callLater(5, dfd.callback, None)

    def _main(self):
        #if self._async_mode:
        #    for _ in range(self._async_concurrency):
        #        dfd = defer.Deferred()
        #        dfd.addBoth(self._main_process_async)
        #        reactor.callLater(0, dfd.callback, None)

        #    reactor.run()
        #else:
        while not stop_condition():
            self._main_process()

class TimingHandler(HandlerBase):
    def __init__(self, handler_key = None):
        super(TimingHandler, self).__init__(handler_key)
        handler_config = mq_settings["handler_configs"][self.__class__.__name__]
        self._elapsed = int(handler_config["elapsed"])
        self._message_key = handler_config.get("input_message", None)

    def _process(self):
        pass

    def _main_process(self):
        if not stop_condition():
            logging.debug(self._log_formatter("start timer", datetime.datetime.now()))
            try:
                self._process()
            except Exception as e:
                logging.error("unexpected exception raised!!!", e = e)
                logging.error("callstack", traceback.format_exc())
            logging.debug(self._log_formatter("ended timer", datetime.datetime.now()))
        if not stop_condition():
            if self._elapsed > 0:
                time.sleep(self._elapsed)

class HandlerRepository(object):
    #stores all inproc message handlers
    global_inproc_handlers = {}

    _initialized = False

    @classmethod
    def _enable_handler(cls, handler_name, handler_config):
        handler_type = handler_config["type"]
        handler_class = _load_object(handler_type)
        handler = handler_class()
        HandlerRepository.global_inproc_handlers[handler_name] = handler

    @classmethod
    def _init(cls):
        '''
        not used: initilize all in-proc handlers will increase unecessary dependencies
        '''

        if HandlerRepository._initialized:
            logging.error("HandlerRepository initialized more than once")
            raise Exception("HandlerRepository initialized more than once")

        for handler_name, handler_config in mq_settings["handler_configs"].items():
            if handler_config.has_key("input_message") and \
            handler_config.get("mode", "viaqueue") == "inproc":
                HandlerRepository._enable_handler(handler_name, handler_config)

        HandlerRepository._initialized = True

    @classmethod
    def process(cls, message_key, message, force_inproc = False, **kw):
        #if not HandlerRepository._initialized:
        #    HandlerRepository._init()

        message = HandlerRepository._fill_message(message_key, message, **kw)

        for handler_name, handler_config in mq_settings["handler_configs"].items():
            if handler_config.get("enabled", True) \
            and handler_config.has_key("input_message") and handler_config["input_message"] == message_key:
                if not force_inproc and (not handler_config.has_key("mode") or handler_config["mode"] == "viaqueue"):
                    mqclient().publish(message_type=message_key, message = message, priority = message.get("__priority", 0))
                    return None
                elif force_inproc or handler_config["mode"] == "inproc":
                    if not HandlerRepository.global_inproc_handlers.has_key(handler_name):
                        HandlerRepository._enable_handler(handler_name, handler_config)
                    result = HandlerRepository.global_inproc_handlers[handler_name].process(message)
                    return result

        raise Exception("message handler is not found for message_key:" % message_key)

    @classmethod
    def _fill_message(cls, message_key, message = None, **kw):
        message_fields = mq_settings["message_configs"][message_key]["message_fields"]

        if message is None:
            message = {}
        for key, value in kw.items():
            message[key] = value

        if len(message) == 0:
            raise Exception("empty message not allowed")

        for field in message_fields["required"]:
            if not message.has_key(field):
                raise Exception("required field %s for message key %s not found" % (field, message_key))

        for field, value in message_fields.get("optional", {}).items():
            if not message.has_key(field):
                message[field] = value

        return message
