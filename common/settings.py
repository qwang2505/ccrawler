# -*- coding: utf-8 -*-

'''
Created on June, 27, 2012

@author: dhcui
'''

import os
import stat
import sys
#import re

import ccrawler.utils.dictconfig as dictconfig
import ccrawler.messagequeue.rabbitmq_blocking_client as rabbitmq_blocking_client
import ccrawler.cache.redis_client as redis_client
import ccrawler.common.configuration as configuration
from ccrawler.utils.log import logging

def _update_dict(target_dict,src_dict):
    for key,value in src_dict.items():
        if key.startswith("##"):
            key = key[2:]
            force_override = True
        else:
            force_override = False

        if not force_override and target_dict.has_key(key) and \
            isinstance(target_dict[key],dict) and isinstance(value,dict):
            _update_dict(target_dict[key],value)
        else:
            target_dict[key] = value

def override_settings(module):
    '''
    Note: make sure this api is called only once and before any business logic.
    '''

    for name,value in module.__dict__.items():
        if not name.startswith("__"):
            module = sys.modules[__name__]
            if hasattr(module,name):
                old_value = getattr(module,name)
                if isinstance(old_value,dict) and isinstance(value,dict):
                    _update_dict(old_value,value)
                else:
                    setattr(module,name,value)
            else:
                setattr(module,name,value)

    config_log()

########################below are message and handler related configs##################################

Crawler_Priorities = configuration.Crawler_Priorities

mq_settings = {
    "message_configs" : {
        "__default_message_config" : {
            "priority_level" : 1,
            "group_mode" : False,
            "group_counts" : None,
            "exchange_type" : "topic",
            "timestamp_expires" : False,
            "message_ids" : None,
            "content_type" : "text/json",
            "persistent" : True,
            "auto_ack" : False,
            "with_timestamp" : False,
            "x_message_ttl" : 1000 * 60 * 60 * 24 * 1,
            "delete_first" : False,
            "durable" : True,
            "exclusive" : False,
            "auto_delete" : False,
            "timeout" : None,
            "rpc_queue_expires" : 1000 * 60 * 60 * 12,
            "max_rpc_queue_count" : 1000,
            "rpc_reply_content_type" : "text/json",
        },
        "__internal_crawler_request" : {
            "exchange" : "exchange__internal_crawler_request", #default: exchange_message_type
            "queue" : "queue__internal_crawler_request", #default: queue_message_type
            "binding_prefix" : "binding__internal_crawler_request", #default: binding_message_type
            "priority_level" : Crawler_Priorities.Total,
            "group_mode" : True,
            "group_counts" : [2, 11, 31, 101], # 1 <= len(group_counts) <= priority_level, group_counts[i] >= 1
            "with_timestamp" : True,
            "message_fields" : {
                "required" : [
                    "url",
                    "page_last_modified",
                    "meta",
                ],
            }
        },
        "__internal_crawler_response" : {
            "message_fields" : {
                "required" : [
                    "url",
                    "original_url",
                    "page_last_modified",
                    "last_crawled",
                    "status",
                    "doc",
                    "headers",
                    "error_message",
                    "meta",
                ],
            }
        },
        "crawl_response" : {
            "message_fields" : {
                "required" : [
                    "url",
                    "original_url",
                    "page_last_modified",
                    "last_crawled",
                    "status",
                    "doc",
                    "headers",
                    "error_message",
                    "meta",
                ],
            }
        },
        # offline mode:   expected_priority, crawl_depth are useful
        # online  mode:   parent_url are useful
        # parsed  mode:   all are useful
        "crawl_request" : {
            "message_fields" : {
                "required" : [
                    "url",
                    "source",# offline, online, parsed
                ],
                "optional" : {
                    "root_url" : None,# initial root source url when injection
                    "parent_url" : None,# parsed parent url
                    "crawl_priority" : None,
                    "crawl_depth" : None,# pending crawl depth
                }
            }
        },
    },

    #handler key must be class name, type is full type name, type is required,
    #input_message is required for message handler, while "elapsed" is required for timing_handler, timing handler doesn't have input_mesage
    #Note: one message must have just one MessageHandler.
    #Note: you need to add message name to message_types field
    "handler_configs" : {
        "CrawlHandler" : {"type" : "ccrawler.handler.crawl_handler.CrawlHandler","input_message" : "crawl_request","mode" : "inproc","output_messages" : ["__internal_crawler_request"]},
        "StaticCrawlerHandler" : {"type" : "ccrawler.static_crawler.static_crawler_handler.StaticCrawlerHandler", "input_message" : "__internal_crawler_request", "mode" : "viaqueue", "output_messages" : ["__internal_crawl_response"], "settings" : "ccrawler.static_crawler.settings"},
        "MiddlewareHandler" : {"type" : "ccrawler.middlewares.middleware_handler.MiddlewareHandler","input_message" : "__internal_crawler_response","mode" : "inproc","output_messages" : ["crawl_response"]},
        "DefaultCrawlResponseHandler" : {"type" : "ccrawler.handler.default_crawl_response_handler.DefaultCrawlResponseHandler","input_message" : "crawl_response","mode" : "inproc","output_messages" : ["crawl_reqest"]},
    },
    "client_config" : {
        "message_types" : ["crawl_request", "__internal_crawl_request", "__internal_crawl_response", "crawl_response"],
        "parameters" :  configuration.mq_client_config["parameters"],
        "aux_store" : configuration.mq_client_config["aux_store"],
    },
}

hosted_handlers = {}

for key, switch in configuration.handler_switches.items():
    if key in mq_settings['handler_configs']:
        mq_settings["handler_configs"][key]["enabled"] = switch

global_mq_client = None
rabbitmq_blocking_client.config(logging)

def mqclient(reload = False):
    global global_mq_client
    if global_mq_client is not None and not reload:
        return global_mq_client
    else:
        global_mq_client = rabbitmq_blocking_client.RabbitMQBlockingClient(mq_settings["client_config"], mq_settings["message_configs"])
        return global_mq_client

heart_beat_config = configuration.heart_beat_config
for key in heart_beat_config["repair_command"].keys():
    heart_beat_config["repair_command"][key] += " %s &";

from ccrawler.handler.hosted_handler_manager import config as config_handler
config_handler(mqclient, mq_settings, logging, heart_beat_config)

###################################################################################################################################
#Note: all query cond related fields should be covered here.
common_url_info_fields = ["crawl_status"]

database_table_fields = {
    #Notes: url_class, full_domain, crawl_status, last_crawled, doc_infos may also can be moved to urlRepositoryMeta.
    "urlRepository" : [
        "url", "crawl_priority", "crawl_depth", "url_class", "crawl_status", "expires", "full_domain",#basic infos
        "last_crawled", "retry_count", "redirect_count", #crawling infos
        "last_processed",
        "md5", "process_status", "modified_count", "last_modified", "first_modified", #doc infos
        "crawl_type", "root_url", "page_last_modified", "encoding", "encoding_created_time", #moved from meta in common-crawler
        "recrawl_time", "recrawl_duration", "recrawl_priority",
    ], # url_info
    "urlRepositoryMeta" : [
        "source", "parent_url", "original_url", "created_time",
        "crawled_count", "error_messages", "redirect_url",
        "error_type", "error_message", #Note:!! these two are not real db fields
        "last_discovered", "discovered_count", "valid_link_count", "processed_count", "last_finished", "status_last_modified", "comments",
        "doc", "headers", "last_crawl_start_time",
    ],
    "rawDocs" : [
        "url","doc","md5","headers", "process_status",#update for each doc update
    ],
    "urlRedirects" : [
        "url","redirect_url",#set once at insertion
    ],
    "crawlDomainWhitelist" : [
        "domain","domain_type","crawl_priority","crawl_depth",#set once at insertion, domain_type: domain, full_domain, host
        "recrawl_details","recrawl_list",
    ],
    "mobileUrlPatterns" : [
        "regex",#set once at insertion
    ],
    "crawlFeeds" : [
        "url","crawl_depth","crawl_priority",
    ],
    "offlineManipulations" : [
        "manipulation","result","datetime","type",
    ],
}

##########################below are redis cache related configs#####################################
redis_client_config = configuration.redis_client_config

domain_types = ["domain","full_domain","host"]
crawl_statuses  = ["crawling", "alive","notAlive","error","failed"]
redis_data_config = {
    "data_types" : {
        "url" : {
            "content_type" : "redis/hash",
            "id_generator" : "md5", #includes md5, raw, none
            "fields" : [
                ("crawl_status", lambda v : crawl_statuses[int(v)], lambda v : str(crawl_statuses.index(v))),
                #("process_status", lambda v : v == "1", lambda v : "1" if v else "0"),
                ("url_class", lambda v : "list" if v == "1" else "details", lambda v : "1" if v == "list" else "0"),
                ("crawl_depth", int),
                ("crawl_priority", int),
                #("modified_count", int),
                #("retry_count", int),
                #("last_modified", long),
                ("last_crawled", long),
                "md5",
                #("first_modified", long),
                #"full_domain",
            ]
        },
        "url_dedup" : {
            #value: 1: indicates it's a live url(crawling, alive, error), 0: indicates it's not a live url(failed, notAlive)
            "content_type" : "text/plain",
            "id_generator" : "md5",
        },
        "dns" : {
            "content_type" : "text/plain",
            "id_generator" : "raw",
        },
        "robots_txt" : {
            "content_type" : "text/plain",
            "id_generator" : "raw",
        },
        "decoding" : {
            "content_type" : "redis/hash",
            "id_generator" : "raw",
            "raw" : True,
        },
    },
}

global_cache_client = None

def load_cache_client(force=False):
    global global_cache_client
    if global_cache_client is None or force:
        global_cache_client = redis_client.RedisClient(redis_client_config, redis_data_config)

def cache_client():
    global global_cache_client
    if global_cache_client is not None:
        return global_cache_client
    else:
        load_cache_client()
        return global_cache_client

###########################below are logging related configs########################################

#LOG_LEVEL=scrapy.log.DEBUG

log_dir = configuration.log_dir
data_dir = configuration.data_dir

LOGGING = {
    'version': 1,
    'disable_existing_loggers':True,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s %(levelname)s [%(tr_file)s %(tr_func)s %(tr_lineno)s] [%(process)d %(thread)d]: %(message)s'
        },
        'simple': {
            'format': '%(asctime)s %(levelname)s %(module)s %(message)s'
        },
    },
    'handlers': {
        'console':{
            'level':'DEBUG',
            'class':'logging.StreamHandler',
            'formatter': 'verbose'
        },
        'warn':{
            'level':'WARN',
            'class':'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'verbose',
            'filename': log_dir + '/all.warn',
            'when': 'D',
            'backupCount' : 7,
        },
        'err':{
            'level':'ERROR',
            'class':'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'verbose',
            'filename': log_dir + '/all.err',
            'when': 'D',
            'backupCount' : 7,
        },
        'debug':{
            'level':'DEBUG',
            'class':'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'verbose',
            'filename': log_dir + '/all.log',
            'when': 'D',
            'backupCount': 7,
        },
    },
    'loggers': {
        'console': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
        'default' : {
            "handlers" : ["debug","warn","err"],
            "level" : 'DEBUG',
        },
    },
    #'root' : {
    #    "handlers" : ['console', 'debug', 'external'],
    #    "level" : 'DEBUG',
    #    }
}

def config_log():
    if not os.path.exists(log_dir):
        try:
            os.mkdir(log_dir)
        except:
            pass
    logging_handlers = LOGGING['handlers']
    for _,handler in logging_handlers.items():
        if 'filename' in handler:
            filename = handler['filename']
            try:
                if os.path.exists(filename) and os.access(filename,os.W_OK):
                    os.chmod(filename,
                     stat.S_IRUSR | stat.S_IROTH | stat.S_IRGRP
                   | stat.S_IWUSR | stat.S_IWOTH | stat.S_IWGRP)
            except Exception:
                pass

    dictconfig.dictConfig(LOGGING)

config_log()

#########################below are crawl related configs#######################################

strong_politeness = False
db_cache_expiry_duration = 60 * 60 * 24
#weak_consistency will check duplicate crawlings in lazy mode, will cause a bit duplicate crawling, but reduced db operations.
weak_consistency = True

###############################################storage related configs#####################################

crawler_db_config = configuration.crawler_db_config
crawlerMeta_db_config = configuration.crawlerMeta_db_config
diagnostics_db_config = configuration.diagnostics_db_config
transcode_db_config = configuration.transcode_db_config

##############################################crawl policies related configs#########################
core_settings = {
    "total_priority_count" : Crawler_Priorities.Total,
    "general_crawl_policies" : configuration.general_crawl_policies,
    "recrawl_policies" : {
        #modes: all, none, whitelist's recrawl_details/list
        "url_class_policies" : {
            "details" : {
                "mode" : "whitelist",
                "min_recrawl_interval" : 60 * 30,
                "max_recrawl_interval" : 60 * 60 * 24 * 10,
                "max_alive_interval"   : 60 * 60 * 24 * 30,
            },
            "list" : {
                "mode" : "all",
                "min_recrawl_interval" : 60 * 5,
                "max_recrawl_interval" : 60 * 60 * 24 * 5,
                "max_alive_interval"   : 60 * 60 * 24 * 60,
            },
            "undefined" : {
                "mode" : "all",
                "min_recrawl_interval" : 60 * 30,
                "max_recrawl_interval" : 60 * 60 * 24 * 10,
                "max_alive_interval"   : 60 * 60 * 24 * 30,
            },
        },
        "max_retry_count" : 3,
        "retry_wait_duration" : 60 * 60 * 4,
        "max_redirect_count" : 10,
        "redirect_wait_duration" : 60 * 60 * 24 * 2,
    },

    "url_types" : ["domain","subdomain","others"],
    "error_types" : ["crawl_error", "doc_error", "recrawl_error", "redirected", "redirected_filtered", "filtered", "exception"],
    "url_sources" : ["online", "offline", "post_ondemand", "parsed", "external", "redirected"],
    "encoding_expiry_duration" : 60 * 60 * 24 * 7, #one week

    "mobile_url_patterns" : configuration.mobile_url_patterns,
    "negative_url_patterns" : configuration.negative_url_patterns,
    "negative_url_extensions" : configuration.negative_url_extensions,
#filtered url won't be displayed in transcoded url,
    "negative_url_domains" : configuration.filtered_url_domains,
    "dynamic_crawl_policies" : configuration.dynamic_crawl_policies,
#key of crawl_policies are crawl sources,
#key of crawl_depth are url types,
#Note: parsed crawl source no need crawl policy.,
    "crawl_policies" : configuration.crawl_policies,
    "dynamic_black_dict" : configuration.dynamic_black_dict,
    "dynamic_white_dict" : configuration.dynamic_white_dict,
}

page_analysis_config_files = {
    "list_page_classifier.ini" :  os.path.join(data_dir, "list_page_classifier.ini"),
    "list_page_classifier.svm" :  os.path.join(data_dir, "list_page_classifier.svm"),
    "common_transcoder.ini" :  os.path.join(data_dir, "common_transcoder.ini"),
    "detail_extractor.ini" :  os.path.join(data_dir, "detail_extractor.ini"),
    "preprocess_transcoder.ini" :  os.path.join(data_dir, "preprocess_transcoder.ini"),
    "white_list.ini" :  os.path.join(data_dir, "white_list.ini"),
}

page_analysis_logger_prefix = os.path.join(log_dir, "pageanalysis")

policy_objects = {
    "url_analyser" : "ccrawler.policies.default_url_analyser.DefaultUrlAnalyser",
    "url_validator" : "ccrawler.policies.default_url_validator.DefaultUrlValidator",
    "crawl_priority_and_depth_evaluator" : "ccrawler.policies.default_crawl_priority_and_depth_evaluator.DefaultCrawlPriorityAndDepthEvaluator",
    "recrawl_predictor" : "ccrawler.policies.default_recrawl_predictor.DefaultRecrawlPredictor",
    "doc_validator" : "ccrawler.policies.default_doc_validator.DefaultDocValidator",
}

#crawler_msg_meta_fields = ["url_class", "crawl_depth", "crawl_priority", "root_url", "encoding", "encoding_created_time", "crawl_type", "full_domain", "last_modified", "first_modified", "modified_count", "retry_count", "redirect_count", "page_last_modified", "url", "recrawl_priority"]
crawler_msg_meta_fields = ["crawl_depth", "crawl_priority", "root_url", "encoding", "encoding_created_time", "crawl_type", "full_domain", "retry_count", "redirect_count", "url"]
