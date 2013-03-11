'''
Created on Jul 10, 2012

@author: dhcui
'''

import sys
import os
import string
import logging as origin_logging

if hasattr(sys, 'frozen'): #support for py2exe
    _srcfile = "logging%s__init__%s" % (os.sep, __file__[-4:])
elif string.lower(__file__[-4:]) in ['.pyc', '.pyo']:
    _srcfile = __file__[:-4] + '.py'
else:
    _srcfile = __file__
_srcfile = os.path.normcase(_srcfile)

# next bit filched from 1.5.2's inspect.py
def currentframe():
    """Return the frame object for the caller's stack frame."""
    try:
        raise Exception
    except:
        return sys.exc_traceback.tb_frame.f_back

if hasattr(sys, '_getframe'): currentframe = lambda: sys._getframe(0)

def build_message(func):
    def _findCaller():
        """
        Find the stack frame of the caller so that we can note the source
        file name, line number and function name.
        """
        f = currentframe()
        #On some versions of IronPython, currentframe() returns None if
        #IronPython isn't run with -X:Frames.
        if f is not None:
            f = f.f_back
        rv = "(unknown file)", 0, "(unknown function)"
        while hasattr(f, "f_code"):
            co = f.f_code
            filename = os.path.normcase(co.co_filename)
            if filename == _srcfile:
                f = f.f_back
                continue
            filename = os.path.basename(filename)
            rv = (filename, f.f_lineno, co.co_name)
            break
        return rv

    def _build_message(*args, **kwargs):
        if len(args) < 2:
            raise Exception("this decorator can be used for Logger only")
        cls = args[0]
        __message = args[1]
        args = args[2:]
        whole_message = cls.format_msg(__message, *args, **kwargs)

        caller = _findCaller()
        extra = {"tr_file" : caller[0], "tr_func" : caller[2], "tr_lineno" : caller[1]}
        func(cls, whole_message, extra)
    return _build_message

class Logger(object):
    _logger = origin_logging.getLogger("default")

    @classmethod
    def format_msg(cls, __message, *args, **kwargs):
        args_msg = ', '.join([str(arg) for arg in args])
        kwargs_msg = ', '.join(["%s: %s" % (pair[0], pair[1]) for pair in kwargs.items()])
        if len(args_msg) == 0:
            merged_msg = kwargs_msg
        elif len(kwargs_msg) == 0:
            merged_msg = args_msg
        else:
            merged_msg = "%s; %s" % (args_msg, kwargs_msg)
        whole_message = __message
        if len(merged_msg) > 0:
            whole_message += ": " + merged_msg
        return whole_message

    @classmethod
    @build_message
    def debug(cls, __message, *args, **kwargs):
        Logger._logger.debug(__message, extra=args[0])

    @classmethod
    @build_message
    def info(cls, __message, *args, **kwargs):
        Logger._logger.info(__message, extra=args[0])

    @classmethod
    @build_message
    def warn(cls, __message, *args, **kwargs):
        Logger._logger.warn(__message, extra=args[0])

    @classmethod
    @build_message
    def error(cls, __message, *args, **kwargs):
        Logger._logger.error(__message, extra=args[0])

    @classmethod
    @build_message
    def critical(cls, __message, *args, **kwargs):
        Logger._logger.critical(__message, extra=args[0])

    @classmethod
    @build_message
    def fatal(cls, __message, *args, **kwargs):
        Logger._logger.fatal(__message, extra=args[0])

logging = Logger()