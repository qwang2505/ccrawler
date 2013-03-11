'''
Created on: Mar 11, 2013

@author: qwang

Default crawl reponse. You might want to use your own crawl response handler to
do specific work.
'''
from ccrawler.handler import handler

class DefaultCrawlResponseHandler(handler.MessageHandler):

    def _process(self, result):
        print result['doc']
