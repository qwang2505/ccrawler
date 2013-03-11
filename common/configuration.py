import re

class Crawler_Priorities(object):
    Highest = 0
    High = 1
    Normal = 2
    Low = 3
    Total = 4

log_dir = "/var/app/transcode/log/"
data_dir = "./data"

general_crawl_policies = {
    "url_match_target" : "all",# modes: whitelist, parent_url, all, none
    "url_match_domain_type" : "full_domain",# modes: host, full_domain, domain
    "external_crawl_mode" : "new",# modes: continue, new
    "domain_based_crawl_priority_and_depth" : True,
    "crawl_in_details" : False,
    "supported_schemes" : ["http","https"],
    "supported_content_types" : ["text/html","application/xhtml+xml"],
    "preemptive_priority_promotion" : True,
    "max_url_length" : 1024,
}

dynamic_crawl_policies = {
    "dynamic_crawl_enabled" : True,
    "double_check_static_crawl_type" : False,
    "double_check_dynamic_crawl_type" :True,
}

dynamic_black_dict ={
             'news.sina.com.cn':{
                                '/c':1
                                }
             }


dynamic_white_dict ={
             'news.xinhuanet.com':{
                '/edu':1,
                '/lianzheng':1
                                   },
             'blog.ifeng.com':{
                 '*':1,
                 '/article':1,
                               },
             'home.news.cn':{
                 '*':1,
                 '/blog':1,
                             }
             }

#==================================below are crawler selction policies
mobile_url_patterns = [
    re.compile(r"^http://m\.baidu\.com"),
    re.compile(r"^http://3g\."), re.compile(r"^http://m\."), re.compile(r"^http://wap\."),
    re.compile(r"^http://\w+\.sina\.cn"), re.compile(r"^http://weibo\.cn"), re.compile(r"^http://info50\.3g\.qq\.com"),
    re.compile(r"^http://i\.ifeng\.com"),
]

negative_url_patterns = [
    re.compile("^http://.+/rss(\.aspx)?$"),
]

negative_url_extensions = [
    ".jpg", ".png", ".bmp", ".gif",
    ".txt", ".xml", ".js", ".tsv", ".csv", ".pdf", ".ps", ".doc", ".docx", ".ppt", ".pptx", ".xls",
    ".rar", ".zip", ".gz", ".tar", ".exe", ".dll", ".deb", ".apk",
    ".rm", ".wav", ".avi", ".mp3", "mid", ".mpg", ".swf", ".mov",
]

#filtered url won't be displayed in transcoded url
filtered_url_domains = ['allyes.com', 'ad-plus.cn']

#key of crawl_policies are crawl sources
#key of crawl_depth are url types
#Note: parsed crawl source no need crawl policy.
crawl_policies = {
    "offline" :  {
        "crawl_priority" : Crawler_Priorities.High,
        "crawl_depth" : {"domain" : 0,"subdomain" : 0,"others" : 0}},
    "online" :   {
        "crawl_priority" : Crawler_Priorities.High,
        "crawl_depth" : {"domain" : 0,"subdomain" : 0,"others" : 0}},
    "post_ondemand" :   {
        "crawl_priority" : Crawler_Priorities.Normal,
        "crawl_depth" : {"domain" : 0,"subdomain" : 0,"others" : 0}},
    "external" : {
        "crawl_priority" : Crawler_Priorities.Low,
        "crawl_depth" : {"domain" : 0,"subdomain" : 0,"others" : 0}},
    "redirected" : {
        "crawl_priority" : Crawler_Priorities.Low,
        "crawl_depth" : {"domain" : 0,"subdomain" : 0,"others" : 0}},
}

handler_switches = {
    "MiddlewareHandler" : True,
    "DefaultCrawlResponseHandler" : True,
    "CrawlerResponseHandler" : False,
    "CrawlHandler" : True,
    "StaticCrawlerHandler" : True,
    "SortScheduler" : False,
}

redis_client_config = {
    "host" : '127.0.0.1', "port" : 6379, "db" : 0, "valid_key" : "__valid_redis", "validation_enabled" : True, "enabled" : True,
}

mq_client_config = {
    "parameters" : {"host" : "localhost","port" : 5672, "virtual_host" : "/","lazy_load" : True},
    "aux_store" : {"enabled" : False, "host" : "localhost", "port" : 27017, "name" : "mq_aux_store"},
}

crawler_db_config = {
    "database_server" : "localhost",
    "database_name" : "crawler",
}

crawlerMeta_db_config = {
    "database_server" : "localhost",
    "database_name" : "crawlerMeta",
}

transcode_db_config = {
    "database_server" : "localhost",
    "database_name" : "transcode",
}

diagnostics_db_config = {
    "database_server" : "localhost",
    "database_name" : "diagnostics",
}

heart_beat_config = {
    "client_enabled" : True,
    "client_class" : "heartbeat.HeartBeatClient",
    "client_interval" : 60 * 5,
    "server_address" : "localhost",
    "server_port" : 9090,
    "max_data_size" : 1024 * 10,
    "backlog" : 1024,
    "server_interval" : 60 * 5,
    "database_server" : "localhost",
    "database_name" : "heartbeat",
    "check_duration" : 60 * 12,
    "notification_duration" : 60 * 30,
    "required_handlers" : ["Processor","RecrawlScheduler","Crawler"],
    "email_server" : "exchdag.baina.com",
    "email_from" : "dhcui@bainainfo.com",
    "email_tos" : ["dhcui@bainainfo.com"],
    "email_title" : "transcode heartbeat failure",
    "handler_name_dict" :{
                          "processor" : ["Processor","processor_concurrency"] ,
                          "scheduler" : ["RecrawlScheduler","scheduler_concurrency"],
                          "crawler" : ["Crawler","crawler_concurrency"] ,
                          "dynamic_crawler" : ["DynamicCrawler","dynamic_crawler_concurrency"]
                          },
    "config_path" : "/home/baina/workspace/dolphin-transcode/trunk/conf/bj-test/bj-test.cfg",
    "detail_flag" : "detail",# another is sections
    "repair_flag" : True,
    "repair_command":{
                     "Processor":"python /var/app/transcode/enabled/processor/processor/start.py Processor",
                     "RecrawlScheduler":"python /var/app/transcode/enabled/scheduler/scheduler/start.py RecrawlScheduler",
                     "DynamicCrawlerHandler":"python /var/app/transcode/enabled/dynamic_crawler/dynamicCrawler/start.py DynamicCrawlerHandler",
                     }
}
