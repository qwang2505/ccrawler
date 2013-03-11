'''
Created on Jan, 14th, 2013

@author: dhcui
'''

import atexit

import libpageanalysis as pa
from ccrawler.utils.log import logging
import ccrawler.common.settings as common_settings

_pa_initialized = False

def _load():
    global _pa_initialized
    if not _pa_initialized:
        _pa_initialized = pa.init(common_settings.page_analysis_logger_prefix, common_settings.page_analysis_config_files)
        if not _pa_initialized:
            logging.error("page_analysis lib can't be loaded")
            return False
        else:
            logging.info("page_analysis lib has been loaded")

    return True

def _preprocess(url, html):
    if not _load():
        return False, None, None

    if isinstance(url, unicode):
        url = url.encode("utf-8", "ignore")

    if not isinstance(url, str):
        raise Exception("url type should be str")

    if isinstance(html, unicode):
        html = html.encode("utf-8", "ignore")

    if not isinstance(html, str):
        raise Exception("html type should be str")

    return True, url, html

def is_list_page(url, html):
    success, url, html = _preprocess(url, html)
    if not success:
        return False, None

    is_list = pa.classify_list_page(url, html)
    return True, is_list

def extract_body_and_title(url, html):
    success, url, html = _preprocess(url, html)
    if not success:
        return False, None, None

    result = pa.extract_body_and_title(url, html)
    return result.success, result.body, result.title

@atexit.register
def _unload():
    global _pa_initialized
    _pa_initialized = False
    pa.reset()
    logging.info("page_analysis lib has been unloaded")

