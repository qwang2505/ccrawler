'''
Created on Feb, 22th, 2013

@author dhcui
'''

class IPolicyBase(object):
    def __init__(self, settings):
        self._settings = settings

class IUrlAnalyser(IPolicyBase):
    def is_mobile_url(self, url):
        pass

    def is_domain_url(self, url):
        pass

    def is_external_url(self, url, parent_url):
        pass

    def normalize_url(self, url, base_url = None):
        pass

    def match_url_domain_info(self, source_info, target_info):
        pass

    def get_url_domain(self, domain_info):
        pass

    def get_crawl_domain_info(self, url):
        pass

    def get_url_type(self, url):
        pass

class IUrlValidator(IPolicyBase):
    '''
    checks if the url is a valid crawl url, returns True if it's valid.
    '''

    def validate(self, url, parent_url, extras = None):
        pass

class ICrawlPriorityAndDepthEvaluator(IPolicyBase):
    '''
    determines static crawl priority for new external urls, redirect urls, extracted urls from doc;
    returns is_valid, crawl_priority, crawl_depth
    '''

    def evaluate(self, url, source, url_info, extras = None):
        pass

class IRecrawlPredictor(IPolicyBase):
    '''
    predicts and returns crawl_status, recrawl time, duration, and priority
    '''

    def predict(self, url, url_info, extras = None):
        pass

class IDocValidator(IPolicyBase):
    '''
    check if the doc is valid, returns True if it's valid
    '''

    def validate(self, url, html, headers, extras = None):
        pass

class IUrlRetireEvaluator(IPolicyBase):
    '''
    Returns if the urls should be retired, i.e., changed to notAlive status, currently, RecrawlPredictor can do this.
    Note: no default implementation now.
    '''

    def evaluate(self, url, url_info, extras = None):
        pass
