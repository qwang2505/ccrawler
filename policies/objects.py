'''
Created on Feb, 22th, 2013

@author dhcui
'''

import ccrawler.common.settings as common_settings
import ccrawler.utils.misc as misc

def _create_policy_object(name):
    type_path = common_settings.policy_objects[name]
    type_object = misc.load_object(type_path)
    return type_object(common_settings.core_settings)

url_analyser =      _create_policy_object("url_analyser")
url_validator =     _create_policy_object("url_validator")
crawl_priority_and_depth_evaluator = _create_policy_object("crawl_priority_and_depth_evaluator")
recrawl_predictor = _create_policy_object("recrawl_predictor")
doc_validator =     _create_policy_object("doc_validator")
