import urllib2
import socket
import gzip
import cStringIO

#from twisted.internet import reactor

from scrapy.http import Response, Request
from scrapy.utils.defer import mustbe_deferred, defer_fail
from scrapy.core.downloader.webclient import ScrapyHTTPClientFactory
from scrapy.core.downloader.handlers.http import HttpDownloadHandler

from ccrawler.utils.log import logging
import ccrawler.static_crawler.dns_cache as dns_cache
import ccrawler.static_crawler.robotstxt as robotstxt
import ccrawler.utils.misc as misc
import ccrawler.static_crawler.redirect as redirect

class Downloader(object):
    def crawl(self, async_mode, url, timeout, request_header, robotstxt_enabled, meta):
        success, result = Downloader.preprocess(url, robotstxt_enabled)
        if not success:
            if async_mode:
                result = defer_fail(result)
        else:
            if async_mode:
                result = self._crawl_async(url, timeout, request_header, meta)
            else:
                result = self._crawl_sync(url, timeout, request_header, meta)

        return misc.postprocess(async_mode, result, Downloader.postprocess)

    def _crawl_async(self, url, timeout, request_header, meta):
        pass

    def _crawl_sync(self, url, timeout, request_header, meta):
        pass

    @classmethod
    def preprocess(cls, url, robotstxt_enabled):
        #initialize result
        result = {"url" : url, "status" : 600, "doc" : None, "headers" : None}
        parsed_result = misc.parse_url(url)
        if parsed_result is None:
            result["error_message"] = "parse url failed"
            return False, result

        #check robots.txt
        if robotstxt_enabled and not robotstxt.allowed_url(url, user_agent or "", parsed_result.scheme, parsed_result.netloc):
            result["error_message"] = "robots.txt disallowed"
            return False, result

        return True, result

    @classmethod
    def postprocess(cls, result):
        if not isinstance(result, dict):
            logging.error("internal exception raised", type = type(result), result = result)
            return {"status" : 600, "error_message" : "internal exception raised %s" % result}
        if result.has_key("error_message") or result["status"] != 200 or result["doc"] is None:
            return result

        #dns cache
        actual_url = result["url"]

        if result["meta"].get("dns_cache_enabled", False):
            if actual_url != result["meta"]["url"]:
                parsed_result = misc.parse_url(actual_url)
                if parsed_result is not None and dns_cache.has_dns_cache(parsed_result.netloc):
                    ip = socket.gethostbyname(parsed_result.netloc)
                    dns_cache.set_dns_cache(parsed_result.netloc, ip)

        #compression
        body = result["doc"]
        ce=result["headers"].get('Content-Encoding',None)
        if ce and ce.lower().find('gzip')!=-1:
            body=cStringIO.StringIO(body)
            body=gzip.GzipFile(fileobj=body,mode='rb').read()

        #chunked transfer encoding
        if result["meta"].get("chunked_transfer_decoding", False) and result["headers"].get('Transfer-Encoding') == 'chunked':
            body = Downloader.decode_chunked_transfer(body)

        #create result dict
        result["doc"] = body
        return result

    @classmethod
    def decode_chunked_transfer(cls, chunked_body):
        body, h, t = '', '', chunked_body
        while t:
            result = t.split('\r\n', 1)
            if len(result) >= 2:
                h, t = result
            else:
                break
            if h == '0':
                break
            size = int(h, 16)
            body += t[:size]
            t = t[size+2:]
        return body

class TwistedDownloader(Downloader):
    """
    TODO: redirecting
    """

    def __init__(self):
        self._handler = HttpDownloadHandler(ScrapyHTTPClientFactory)
        redirect_settings = {
            "REDIRECT_MAX_METAREFRESH_DELAY" : 5,
            "REDIRECT_MAX_TIMES" : 2,
            "REDIRECT_PRIORITY_ADJUST" : 0,
        }
        self._redirect_middleware = redirect.RedirectMiddleware(redirect_settings)

    def _crawl_async(self, url, timeout, request_header, meta):
        meta["download_timeout"] = timeout
        request = Request(url = url, meta=meta)
        for key, value in request_header.items():
            request.headers.setdefault(key, value)

        return self._crawl(request)

    def _crawl(self, request):
        dfd = mustbe_deferred(self._handler.download_request, request, None)
        def downloaded(response, request):
            result = {"status" : 600, "doc" : None, "headers" : None}
            if isinstance(response, Response):
                response = self._redirect_middleware.process_response(request, response, None)
                if isinstance(response, Response):
                    result["url"] = response.url
                    result["status"] = response.status
                    result["doc"] = response.body
                    result["headers"] = response.headers
                    result["meta"] = request.meta
                elif isinstance(response, Request):
                    redirect_time = response.meta.get("redirect_time", 0)
                    redirect_time += 1
                    if redirect_time >= 2:
                        result["url"] = response.url
                        result["status"] = 601
                        result["meta"] = response.meta
                    else:
                        result["url"] = response.url
                        result["status"] = 700
                        result["meta"] = response.meta
                        result["redirect_time"] = redirect_time
                else:
                    raise Exception("not supported")
            else:
                result["url"] = request.url
                result["error_message"] = "crawler failed:%s" % response
                result["meta"] = request.meta

            return result

        return dfd.addBoth(downloaded, request)

    def _crawl_sync(self, url, timeout, request_header, meta):
        raise Exception("not supported")

class UrlLib2Downloader(Downloader):
    def __init__(self):
        pass

    def _crawl_sync(self, url, timeout, request_header, meta):
        result = {"url" : url, "status" : 600, "doc" : None, "headers" : None, "meta" : meta}

        req = urllib2.Request(url)

        #set headers
        for key, value in request_header.items():
            req.add_header(key, value)

        #set timeout
        try:
            response=urllib2.urlopen(req, timeout=timeout)
        except Exception as e:
            error_message = misc.exception_to_str(e)
            if error_message.find("HTTP Error 304: Not Modified") != -1:
                result["status"] = 304
                return result
            else:
                result["error_message"] = error_message
                logging.error("static_crawl failed when opening url", url = url, exception = e)
                return result

        try:
            body = response.read()
        except Exception as e:
            result["error_message"] = str(e)
            logging.error("static_crawl failed when reading response", url = url, exception = e)
            return result

        result["url"] = response.url
        result["status"] = response.code
        result["doc"] = body
        result["headers"] = response.headers
        return result

    def _crawl_async(self, url, timeout, request_header, meta):
        raise Exception("not supported")
        #result = self._crawl_sync(url, timeout, request_header)
        #callback(result)
