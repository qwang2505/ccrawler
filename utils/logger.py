'''
Created on Dec 6, 2011

@author: qwang

This module is a simple logging module for dolphin-transcode.
Generally there has a default logger for every application,
but modules also can log with specific logger as long as been
configured. Loggers should be configured as application started,
in settings file or others.
'''

import logging

from logging import Logger as OriginalLogger
from ccrawler.utils import dictconfig

NOTSET = 'NOT-SET'

class Logger(OriginalLogger):
    '''
    Custom Logger class.
    '''
    keywords = []

    def __init__(self, name, level=logging.NOTSET):
        return OriginalLogger.__init__(self, name, level)

    def config(self, config):
        self.keywords = config.get('keywords', [])

class WebLogger(Logger):

    def info(self, msg, *args, **kwargs):
        kwargs = self.configKeys(**kwargs)
        return OriginalLogger.info(self, msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        kwargs = self.configKeys(**kwargs)
        return OriginalLogger.warn(self, msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        kwargs = self.configKeys(**kwargs)
        return OriginalLogger.debug(self, msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        return Logger.exception(self, msg, *args)

    def error(self, msg, *args, **kwargs):
        kwargs = self.configKeys(**kwargs)
        return OriginalLogger.error(self, msg, *args, **kwargs)

    def fatal(self, msg, *args, **kwargs):
        kwargs = self.configKeys(**kwargs)
        return OriginalLogger.fatal(self, msg, *args, **kwargs)

    def configKeys(self, **kwargs):
        extra = {}
        for key in self.keywords:
            value = kwargs.get(key, NOTSET)
            extra[key] = value
            if key in kwargs:
                del kwargs[key]
        kwargs['extra'] = extra
        return kwargs

'''
Custom dict configurator.
'''
class CustomDictConfiturator(dictconfig.DictConfigurator):

    '''
    Custom configure_logger method.
    Add 'logger' settings in config and configure custom logger class for logger.
    '''
    def configure_logger(self, name, config, incremental=False):
        custom_logger = config.get('logger', None)
        if custom_logger:
            old_logger = logging._loggerClass
            cls = self.resolve(custom_logger)
            setLoggerClass(cls)
            logger = logging.getLogger(name)
            setLoggerClass(old_logger)
            logger.config(config)
        else:
            logger = logging.getLogger(name)
        self.common_logger_config(logger, config, incremental)
        propagate = config.get('propagate', None)
        if propagate is not None:
            logger.propagate = propagate

'''
Set logger class for python's logging module, so that you can do
something cool in you custom class.
'''
def setLoggerClass(cls):
    logging._loggerClass = cls
    return

def dictConfig(config):
    '''Configure logging using a dictionary.'''
    CustomDictConfiturator(config).configure()
