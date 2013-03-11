#!/usr/bin/python
import sys

from ccrawler.handler.handler_daemon import launch

def start_handler():
    handler_name = sys.argv[2]
    concurrency = 1
    if len(sys.argv) > 3:
        try:
            concurrency = int(sys.argv[3])
        except:
            usage()
            return
    hosted_handlers = {}
    hosted_handlers[handler_name] = {'concurrency': concurrency}
    launch(hosted_handlers)

cmd_map = {
    'start': start_handler,
}

def usage():
    print '''
    Usage:
    1. ccrawler start HANDLER_NAME
    2. ccrawler crawl URL
    TODO complete
    '''

def process():
    cmd = sys.argv[1]
    if cmd not in cmd_map:
        usage()
        return
    func = cmd_map[cmd]
    func()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
        sys.exit(-1)
    process()
