from twisted.internet import defer, reactor
from twisted.python import failure

def defer_fail(_failure):
    """Same as twisted.internet.defer.fail, but delay calling errback until
    next reactor loop
    """
    d = defer.Deferred()
    reactor.callLater(0, d.errback, _failure)
    return d

def defer_succeed(result):
    """Same as twsited.internet.defer.succed, but delay calling callback until
    next reactor loop
    """
    d = defer.Deferred()
    reactor.callLater(0, d.callback, result)
    return d

def defer_result(result):
    if isinstance(result, defer.Deferred):
        return result
    elif isinstance(result, failure.Failure):
        return defer_fail(result)
    else:
        return defer_succeed(result)

def mustbe_deferred(f, *args, **kw):
    """Same as twisted.internet.defer.maybeDeferred, but delay calling
    callback/errback to next reactor loop
    """
    try:
        result = f(*args, **kw)
    except Exception, e:
        return defer_fail(failure.Failure(e))
    else:
        return defer_result(result)
    
#notTested
def all_deferred(f, *args, **kw):
    def func(f, d, *args, **kw):
        try:
            result = f(*args, **kw)
        except Exception, e:
            d.errback(failure.Failure(e))
        else:
            d.callback(result)
    d = defer.Deferred()
    reactor.callLater(0, func, f, d, *args, **kw)
    return d

def defer_once(f, *args, **kw):
    _ = defer.Deferred()
    reactor.callLater(0, f, *args, **kw)
    #d.addCallback(f)

def defer_loop(f, *args, **kw):
    defer_once(loop, f, *args, **kw)
