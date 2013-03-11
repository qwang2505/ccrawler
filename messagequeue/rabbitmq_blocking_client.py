'''
Created on June, 27, 2012

@author: dhcui
'''

import calendar
import datetime
import time
import copy
import simplejson
import socket
import errno
import uuid
import threading
import Queue

import pika

#from transcode.utils.log import logging
#from transcode.utils.format import datetime2timestamp
#import transcode.messagequeue.mq_aux_store as mq_aux_store
#import transcode.utils.misc as misc

logging = None

def config(logging_obj):
    global logging
    logging = logging_obj

class MqAuxStore(object):
    '''
    mock implementation: remove dependencies for pymongo
    '''

    def __init__(self, host, port, name):
        pass

    def check_message_expires(self, message_type, message_id, timestamp):
        return False

    def add_expired_message(self, message_type, message_id, timestamp):
        pass

def datetime2timestamp(dt):
    '''
    Converts a datetime object to UNIX timestamp in milliseconds.
    '''
    if hasattr(dt, 'utctimetuple'):
        t = calendar.timegm(dt.utctimetuple())
        timestamp = int(t) + dt.microsecond / 1000000.0
        return int(timestamp * 1000)
    return dt

def dumps_jsonx(dict_obj):
    appends = []
    for key, value in dict_obj.items():
        if key.endswith("__"):
            if not isinstance(value, str):
                raise Exception("append fields must be str, %s" % key)
            dict_obj[key] = [len(appends), len(value)]
            appends.append(value)

    main = simplejson.dumps(dict_obj)
    if len(main) > 0xffffffff:
        raise Exception("dumps_jsonx length exceeded 0xffffffff")

    prefix = "%08x" % (len(main))
    return ''.join([prefix, main] + appends)

def loads_jsonx(str_obj):
    prefix = str_obj[:8]
    str_obj = str_obj[8:]
    main_length = int(prefix, 16)
    main = str_obj[:main_length]
    str_obj = str_obj[main_length:]
    main_obj = simplejson.loads(main)
    appends = sorted(filter(lambda pair : pair[0].endswith("__"), main_obj.items()), key = lambda pair: pair[1][0])
    for i in range(len(appends)):
        key, value = appends[i]
        length = value[1]
        content = str_obj[:length]
        str_obj = str_obj[length:]
        main_obj[key] = content

    return main_obj

def reliable_op(func):
    """
    if message queue is broken, will try to reconnect...
    """

    def _reconnect(args, msg, e):
        if len(args) == 0 or not isinstance(args[0], RabbitMQBlockingClient):
            raise Exception("unsupported decorator, it should be applied to RabbitMQBlockingClient methods")
        self = args[0]

        while self._stop_condition is None or not self._stop_condition():
            logging.error("message queue connection error: %s, %s reconnecting..." % (msg, e))
            time.sleep(5)
            try:
                self._init(self._client_configs, self._message_configs)
                logging.debug("message queue reconnection succeeded")
                return True
            except pika.exceptions.AMQPConnectionError, e:
                pass
            except pika.exceptions.AMQPChannelError, e:
                if not _reconnect(args, msg, e):
                    raise
            except socket.error as e:
                if e.errno != errno.ECONNREFUSED:
                    raise

        return False

    def _reliable_op(*args, **kwargs):
        argnames = func.func_code.co_varnames[:func.func_code.co_argcount]
        fname = func.func_name
        msg = 'MQ - -> %s(%s)' % (fname, ','.join('%s=%s' % entry for entry in zip(argnames[1:], args[1:]) + kwargs.items()))

        while True:
            try:
                return func(*args, **kwargs)
            except pika.exceptions.AMQPConnectionError, e:
                if not _reconnect(args, msg, e):
                    raise
            except pika.exceptions.AMQPChannelError, e:
                if not _reconnect(args, msg, e):
                    raise
            except socket.error as e:
                if e.errno == errno.ECONNREFUSED:
                    if not _reconnect(args, msg, e):
                        raise
                else:
                    raise 

    return _reliable_op

def generate_message_id(message, message_id_fields):
    id_values = [message[id_field] for id_field in message_id_fields]
    return "##**@@".join(id_values)

class RabbitMQBlockingConnection(object):
    def __init__(self, host='localhost', port=5672, virtual_host='/', credentials=None, channel_max=0, frame_max=131072, heartbeat=False):

        parameters = pika.connection.ConnectionParameters(host=host, port=port, virtual_host=virtual_host)#, credentials=credentials, channel_max=channel_max, frame_max=frame_max, heartbeat=heartbeat)
        self._connection = pika.adapters.BlockingConnection(parameters)
        self.channel = self._connection.channel()

    def is_closed(self):
        return self._connection is None or not self._connection.is_open or self.channel is None

    def close(self):
        if self._connection is not None:
            self._connection.close()
        self._connection = None
        self.channel = None

class RabbitMQBlockingProxy(object):
    '''
    builtin message fields: __delivery_tag, __priority, __timestamp, __group_hash, __body, __reply_to, __correlation_id
    '''

    def __init__(self, blocking_connection, aux_store, message_config):
        self._blocking_connection = blocking_connection
        self._channel = self._blocking_connection.channel
        self._aux_store = aux_store
        self._message_config = message_config
        for key, value in self._message_config.items():
            setattr(self, "_" + key, value)

        self._rpc_queue_pool = Queue.Queue()
        self._rpc_queue_count = 0
        self._rpc_queue_count_lock = threading.Lock()
        self._rpc_cache = {}

        if self._delete_first:
            try:
                self._channel.exchange_delete(exchange=self._exchange)
            except pika.exceptions.AMQPChannelError:
                pass
        self._channel.exchange_declare(exchange=self._exchange, type=self._exchange_type, durable=self._durable)

        if self._priority_level <= 0:
            raise Exception("priority_level should be >= 1")

        if self._group_mode:
            if self._group_counts is None or len(self._group_counts) == 0:
                raise Exception("group_counts should not be empty if group_mode is true")
            if min(self._group_counts) <= 0:
                raise Exception("any group_count should be >= 1")
            self._curr_group_cursors = [0 for _ in range(self._priority_level)]

            start = len(self._group_counts)
            for group_id in range(start, self._priority_level):
                self._group_counts[group_id] = self._group_counts[start - 1]

        for priority in range(self._priority_level):
            if self._group_mode:
                group_count = self._group_counts[priority]
            else:
                group_count = 1

            #TODO: P2. maybe we can use lazy declare for queues for better memory usage.
            for group_id in range(group_count):
                curr_queue_name = self.generate_routing_name(self._queue_name, priority, group_id)
                curr_routing_key = self.generate_routing_name(self._binding_prefix, priority, group_id)
                self._connect_queue(curr_queue_name, self._exchange, curr_routing_key,
                    self._delete_first, self._durable, self._exclusive, self._auto_delete, self._x_message_ttl)

    def generate_routing_name(self, prefix, priority, group_id):
        name = prefix
        if self._priority_level > 1: 
            name += ".P" + str(priority)
        if self._group_mode:
            name += ".G" + str(group_id)
        return name

    def _connect_queue(self, queue_name, exchange, routing_key, delete_first = False,
        durable=True, exclusive=False, auto_delete=False, x_message_ttl=None):
        if delete_first:
            try:
                self._channel.queue_delete(queue=queue_name)
            except pika.exceptions.AMQPChannelError:
                pass

        arguments = {}
        if x_message_ttl is not None:
            arguments["x-message-ttl"] = x_message_ttl

        self._channel.queue_declare(queue=queue_name, durable=durable, exclusive=exclusive, auto_delete=auto_delete, arguments=arguments)
        self._channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routing_key)

    def _get_by_queue(self, queue_name):
        method, properties, body = self._channel.basic_get(queue=queue_name, no_ack=self._auto_ack)
        if method is None or method.NAME == "Basic.GetEmpty":
            return None

        if properties.content_type == "text/json":
            message = loads_jsonx(body)
        else:
            message = {"__body" : body}

        message["__delivery_tag"] = method.delivery_tag
        if properties.timestamp is not None:
            message["__timestamp"] = properties.timestamp

        if self._timestamp_expires:
            message_id = generate_message_id(message, self._message_ids)
            if self._aux_store is None:
                expired = False
                logging.warn("aux_store is none in basic_get")
            else:
                expired = self._aux_store.check_message_expires(self._message_type, message_id, properties.timestamp)
            if expired:
                if not self._auto_ack:
                    self.ack(method.delivery_tag)
                return self._get_by_queue(queue_name)

        return message

    def _get_curr_group_id(self, priority):
        if self._group_mode:
            curr_group_id = self._curr_group_cursors[priority] % self._group_counts[priority]
            self._curr_group_cursors[priority] = (self._curr_group_cursors[priority] + 1) % self._group_counts[priority]
        else:
            curr_group_id = 0

        return curr_group_id

    def _get_by_priority(self, priority):
        #this start_group_id initial value is a value that make the first curr_group_id == start_group_id false always
        start_group_id = self._get_curr_group_id(priority)
        curr_group_id = start_group_id

        while True:
            queue_name = self.generate_routing_name(self._queue_name, priority, curr_group_id)
            message = self._get_by_queue(queue_name)
            if message is not None:
                return message
            else:
                curr_group_id = self._get_curr_group_id(priority)
                if curr_group_id == start_group_id:
                    return None

    def get(self):
        for priority in range(self._priority_level):
            message = self._get_by_priority(priority)
            if message is not None:
                return message
        return None

    def ack(self, delivery_tag):
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def publish(self, message, priority = 0):
        if priority < 0:
            raise Exception("priority %d should be >= 0" % priority)

        if priority >= self._priority_level:
            logging.warn("priority has been changed due to exceeding", message = message, original_priority = priority, changed_priority = self._priority_level - 1)
            priority = self._priority_level - 1

        #determine properties
        properties = pika.BasicProperties()
        properties.content_type = self._content_type
        if self._persistent:
            properties.delivery_mode = 2
        if self._with_timestamp:
            properties.timestamp = datetime2timestamp(datetime.datetime.utcnow())

        #get group_id
        if self._group_mode:
            if self._content_type != "text/json":
                group_hash = message #Note: here we just support hashable object, e.g., str/unicode etc.
            else:
                if not message.has_key("__group_hash"):
                    raise Exception("__group_hash is required if message type is in group_mode")
                group_hash = message["__group_hash"]
            group_int_hash = hash(group_hash)
            group_id = group_int_hash % self._group_counts[priority]
        else:
            group_id = 0

        if self._content_type == "text/json" and message.has_key("__group_hash"):
            message.pop("__group_hash") #the message is changed here

        #determine body
        if self._content_type == "text/json":
            body = dumps_jsonx(message)
        else:
            body = message

        #determine routing_key
        routing_key=self.generate_routing_name(self._binding_prefix, priority, group_id)

        self._channel.basic_publish(exchange=self._exchange, routing_key=routing_key, body=body, properties=properties)

    def expire_message(self, message):
        if not self._timestamp_expires:
            raise Exception("expire_message should be valid when _timestamp_expires is true")
        if self._aux_store is None:
            logging.warn("aux_store is none in expire_message")
            return
        message_id = generate_message_id(message, self._message_ids)
        timestamp = datetime2timestamp(datetime.datetime.utcnow())
        self._aux_store.add_expired_message(self._message_type, message_id, timestamp)

    def reply(self, message, response):
        properties = pika.BasicProperties(correlation_id = message["__correlation_id"])

        if self._rpc_reply_content_type == "text/json":
            response = dumps_jsonx(response)

        self._channel.basic_publish(exchange="", routing_key=message["__reply_to"], body=response, properties=properties)

    def _get_callback_queue(self, correlation_id, on_response):
        '''
        get callback queue from rpc_queue_pool, and create a new one if pool is empty
        '''

        start_time = datetime.datetime.utcnow()
        curr_time = datetime.datetime.utcnow()
        callback_queue = None
        while callback_queue is None and curr_time - start_time < datetime.timedelta(seconds = self._timeout):
            if self._rpc_queue_count >= self._max_rpc_queue_count:
                try:
                    callback_queue = self._rpc_queue_pool.get(True, self._timeout)
                except:
                    return None
            else:
                try:
                    callback_queue = self._rpc_queue_pool.get(False)
                except:
                    try:
                        result = self._channel.queue_declare(exclusive=False, auto_delete=False, arguments={"x-expires" : self._rpc_queue_expires})
                    except:
                        curr_time = datetime.datetime.utcnow()
                        continue
                    callback_queue = result.method.queue
                    self._rpc_queue_count_lock.acquire()
                    self._rpc_queue_count += 1
                    self._rpc_queue_count_lock.release()

            try:
                self._channel.basic_consume(on_response, no_ack = True, queue = callback_queue, consumer_tag = correlation_id)
            except:
                self._rpc_queue_count_lock.acquire()
                self._rpc_queue_count -= 1
                self._rpc_queue_count_lock.release()
                curr_time = datetime.datetime.utcnow()
                continue
            return callback_queue
        return None

    def rpc(self, message, priority = 0):
        '''
        caller of rpc will wait until results are returned, timeout supported by default
        '''

        start_time = datetime.datetime.utcnow()
        correlation_id = str(uuid.uuid4())

        def on_response(channel, method, props, body):
            if correlation_id == props.correlation_id:
                self._rpc_cache[correlation_id] = body

        callback_queue = self._get_callback_queue(correlation_id, on_response)
        if callback_queue is None:
            logging.warn("can't get callback_queue", url=message.get("url", ""))
            return None

        message["__reply_to"] = callback_queue
        message["__correlation_id"] = correlation_id

        self.publish(message, priority)
        while not self._rpc_cache.has_key(correlation_id):
            curr_time = datetime.datetime.utcnow()
            if curr_time - start_time > datetime.timedelta(seconds = self._timeout):
                logging.warn("timeout for rpc request", url=message.get("url", ""))
                break
            self._blocking_connection._connection.process_data_events()

        self._channel.basic_cancel(consumer_tag = correlation_id)
        self._rpc_queue_pool.put(callback_queue)
        response = self._rpc_cache.pop(correlation_id, None)
        if response is not None and self._rpc_reply_content_type == "text/json":
            response = loads_jsonx(response)

        return response

    #can only be called before destructor
    def dispose(self, delete=False):
        for priority in range(self._priority_level):
            if self._group_mode:
                group_count = self._group_counts[priority]
            else:
                group_count = 1
            for group_id in range(group_count):
                curr_queue_name = self.generate_routing_name(self._queue_name, priority, group_id)
                curr_routing_key = self.generate_routing_name(self._binding_prefix, priority, group_id)
                self._blocking_connection.channel.queue_unbind(exchange=self._exchange, queue=curr_queue_name, routing_key=curr_routing_key)
                if delete:
                    self._blocking_connection.channel.queue_delete(queue=curr_queue_name)

        if delete:
            self._channel.exchange_delete(exchange=self._exchange)

class RabbitMQBlockingClient(object):

    @reliable_op
    def __init__(self, client_config, message_configs):
        self._init(client_config, message_configs)

    def _init(self, client_config, message_configs):
        self._blocking_connection = None
        self._proxies = {}
        parameters = client_config.get("parameters", {})
        self._lazy_load = parameters.get("lazy_load", True)
        self._message_configs = message_configs
        self._client_configs = client_config
        if self._client_configs.has_key("stop_condition"):
            self._stop_condition = self._client_configs["stop_condition"]
        else:
            self._stop_condition = None

        self._connect(parameters)

        aux_store_config = client_config.get("aux_store", {})
        if aux_store_config.get("enabled", False):
            self._aux_store = MqAuxStore(aux_store_config["host"], aux_store_config["port"], aux_store_config["name"])
        else:
            self._aux_store = None

        if not self._lazy_load:
            for message_type in client_config.get("message_types", []):
                self._add_proxy(message_type)

    def _connect(self, parameters = {}):
        host = parameters.get("host", "localhost")
        port = parameters.get("port", 5672)
        virtual_host = parameters.get("virtual_host", "/")
        credentials = parameters.get("credentials", None)
        channel_max = parameters.get("channel_max", 0)
        frame_max = parameters.get("frame_max", 131072)
        heartbeat = parameters.get("heartbeat", False)
        self._blocking_connection = RabbitMQBlockingConnection(host, port, virtual_host, credentials, channel_max, frame_max, heartbeat)

    def _add_proxy(self, message_type):
        message_config = RabbitMQBlockingClient._get_message_config(message_type, self._message_configs)
        proxy = RabbitMQBlockingProxy(self._blocking_connection, self._aux_store, message_config)
        key = message_config["message_type"]
        if self._proxies.has_key(key):
            logging.error("proxy has been added", key)
            raise Exception("proxy has been added", key)
        self._proxies[key] = proxy

    def set_stop_condition(self, stop_condition):
        self._stop_condition = stop_condition
        self._client_configs["stop_condition"] = stop_condition

    @reliable_op
    def get(self, message_type, wait_secs = -1, sleep_secs = 5):
        if not self._proxies.has_key(message_type):
            if self._lazy_load:
                self._add_proxy(message_type)
            else:
                raise Exception("no proxy %s found" % message_type)

        ret = None
        if wait_secs == 0:
            ret = self._proxies[message_type].get()
        else:
            ret = self._proxies[message_type].get()
            start_time = datetime.datetime.utcnow()
            while ret == None and (self._stop_condition == None or not self._stop_condition()) and \
                (wait_secs == -1 or datetime.datetime.utcnow() < start_time + datetime.timedelta(seconds = wait_secs)):
                time.sleep(sleep_secs)
                ret = self._proxies[message_type].get()

        return ret

    @reliable_op
    def ack(self, message_type, delivery_tag):
        if not self._proxies.has_key(message_type):
            if self._lazy_load:
                self._add_proxy(message_type)
            else:
                raise Exception("no proxy %s found" % message_type)

        self._proxies[message_type].ack(delivery_tag)

    @reliable_op
    def publish(self, message_type, message, priority = 0):
        if not self._proxies.has_key(message_type):
            if self._lazy_load:
                self._add_proxy(message_type)
            else:
                raise Exception("no proxy %s found" % message_type)

        return self._proxies[message_type].publish(message, priority)

    def rpc(self, message_type, message, priority = 0):
        if not self._proxies.has_key(message_type):
            if self._lazy_load:
                self._add_proxy(message_type)
            else:
                raise Exception("no proxy %s found" % message_type)

        return self._proxies[message_type].rpc(message, priority)

    def reply(self, message_type, message, response):
        if not self._proxies.has_key(message_type):
            if self._lazy_load:
                self._add_proxy(message_type)
            else:
                raise Exception("no proxy %s found" % message_type)

        return self._proxies[message_type].reply(message, response)

    @reliable_op
    def expire_message(self, message_type, message):
        if not self._proxies.has_key(message_type):
            if self._lazy_load:
                self._add_proxy(message_type)
            else:
                raise Exception("no proxy %s found" % message_type)

        self._proxies[message_type].expire_message(message)

    def is_closed(self):
        return self._blocking_connection.is_closed()

    def close(self):
        self._blocking_connection.close()

    @classmethod
    def set_default(cls, message_config, key, value):
        if not message_config.has_key(key):
            message_config[key] = value
    
    @classmethod
    def validate_message_config(cls, message_config, default_message_config):
        if not message_config.has_key("message_type"):
            raise Exception("message_type is required")
    
        message_type = message_config["message_type"]
        RabbitMQBlockingClient.set_default(message_config, "queue_name", "queue_" + message_type)
        RabbitMQBlockingClient.set_default(message_config, "exchange", "exchange_" + message_type)
        RabbitMQBlockingClient.set_default(message_config, "binding_prefix", "binding_" + message_type)
        for key, value in default_message_config.items():
            RabbitMQBlockingClient.set_default(message_config, key, value)
    
        if message_config["priority_level"] < 0:
            raise Exception("priority_level should >= 0")
    
        if message_config["timestamp_expires"] and (message_config["message_ids"] is None or len(message_config["message_ids"]) == 0):
            raise Exception("message_ids should be assigned if timestamp_filter is true")
    
        if message_config["timestamp_expires"] and not message_config["with_timestamp"]:
            raise Exception("with_timestamp should be true if timestamp_filter is true")

    @classmethod
    def _get_message_config(cls, message_type, message_configs):
        config = {}
        if message_configs.has_key(message_type):
            config = copy.deepcopy(message_configs[message_type])
        else:
            raise Exception("not allowed message type %s" % message_type)
        config["message_type"] = message_type
        RabbitMQBlockingClient.validate_message_config(config, message_configs["__default_message_config"])
        return config

    def clear_proxies(self, delete=False):
        for _, proxy in self._proxies.items():
            proxy.dispose(delete)
        self._proxies.clear()

    def dispose(self, delete=False):
        self.clear_proxies(delete)
