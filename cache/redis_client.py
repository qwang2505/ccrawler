'''
Created on August, 28, 2012

@author: dhcui
'''

import time
import re
import copy

import redis
import redis.exceptions

from ccrawler.utils.log import logging
import ccrawler.utils.misc as misc
from ccrawler.utils.decoder import encode_string

def reliable_op(func):
    """
    if redis is broken, will try to reconnect...
    """

    def _reconnect(args, msg, e):
        if len(args) == 0 or not isinstance(args[0], RedisClient):
            raise Exception("unsupported decorator, it should be applied to RedisClient methods")
        self = args[0]

        while self._stop_condition is None or not self._stop_condition():
            logging.error("redis connection error: %s, %s reconnecting..." % (msg, e))
            time.sleep(5)
            try:
                self._wait()
                return True
            except redis.exceptions.ConnectionError as e:
                pass

        logging.warn("whole process is terminating")
        raise Exception("whole process is terminating")

    def _reliable_op(*args, **kwargs):
        argnames = func.func_code.co_varnames[:func.func_code.co_argcount]
        fname = func.func_name
        msg = 'MQ - -> %s(%s)' % (fname, ','.join('%s=%s' % entry for entry in zip(argnames[1:], args[1:]) + kwargs.items()))

        while True:
            try:
                return func(*args, **kwargs)
            except redis.exceptions.ConnectionError as e:
                _reconnect(args, msg, e)

    return _reliable_op

class RedisClient(object):
    """
    redis/hash:
        we assume all the fields are required and will not be deleted separately.
        raw mode: get/set all fields directly, dynamic fields

    """

    _special_none_pattern = re.compile("^\\\\+None$")

    @reliable_op
    def __init__(self, config, data_config):
        self._init(config, data_config)

    def _init(self, config, data_config):
        self._client = redis.StrictRedis(host=config.get("host", "localhost"), port=config.get("port", 6379), db=config.get("db", 0))
        self._valid_key = config.get("valid_key", "__valid_redis")
        self._stop_condition = None
        self._data_types = data_config.get("data_types", {})
        self._validation_enabled = config.get("validation_enabled", False)
        self._enabled = config.get("enabled", False)

        if self._enabled:
            self._wait()

        logging.debug("redis client initialized")

    def _wait(self):
        if not self._validation_enabled:
            return

        while self._client.get(self._valid_key) != "1": #Notes: all clients will wait until the __valid_redis field is set to 1
            if self._stop_condition is not None and self._stop_condition():
                logging.warn("whole process is terminating")
                raise Exception("whole process is terminating")
            else:
                logging.warn("redis server is loading data")
                time.sleep(5)

    @classmethod
    def _generate_name(cls, id_generator, content_type, data_type, data_key):
        if content_type == "redis/set":#ignores id_generator for redis/set type
            return data_type
        elif id_generator == "raw":
            return ":".join([data_type, data_key])
        elif id_generator == "md5":
            return ":".join([data_type, misc.md5(data_key)])
        elif id_generator == "none":
            return data_type
        else:
            raise Exception("not supported id_generator %s" % id_generator)

    def _prepare_op(self, data_type, data_key):
        if not self._data_types.has_key(data_type):
            raise Exception("unsupported data type, data_type: %s" % data_type)

        if not isinstance(data_type, str):
            data_type = encode_string(data_type)
            if data_type is None:
                raise Exception("data_type must be str, %s" % data_type)

        data_config = self._data_types.get(data_type)
        enabled = data_config.get("enabled", True)
        content_type = data_config.get("content_type", "text/plain")
        name = RedisClient._generate_name(data_config.get("id_generator", "raw"), content_type, data_type, data_key)
        return name, content_type, enabled

    @reliable_op
    def get(self, data_type, ori_data_key, **kwargs):
        """
        text/json: returns dict object
        text/plan: returns str object
        redis/hash: needs fields, strict=False by default, returns dict object, returns None if strict and required field not exists
        redis/set: use data_key as query value, and returns True/False for existence
        """
        #logging.debug("redis get", data_type, ori_data_key, kwargs)
        data_key = copy.deepcopy(ori_data_key)

        if not self._enabled:
            return None

        name, content_type, enabled = self._prepare_op(data_type, data_key)
        if not enabled:
            return None

        parameters = RedisClient._get_parameters("get", content_type, **kwargs)

        if content_type == "text/json":
            data = self._client.get(name)
            return self._load_json_object(data)
        elif content_type == "text/plain":
            return self._client.get(name)
        elif content_type == "redis/hash":
            return self._get_hash(data_type, name, parameters["fields"], parameters["strict"])
        elif content_type == "redis/set":
            if not isinstance(data_key, str):
                data_key = encode_string(data_key)
                if data_key is None:
                    raise Exception("data_key must be str, %s" % data_key)

            return self._client.sismember(name, data_key)
        else:
            raise Exception("unsupported data_type for get, %s" % data_type)

    _operation_parameters = {
        "set" : {
            "defaults" : {"data" : None, "update_map" : {}, "inc_map" : {}, "fields" : [], "strict" : True, "cond" : {}, "nx" : False, "cond" : None},
            "required" : {
                "text/json" : ["data", "nx"],
                "text/plain" : ["data", "nx"],
                "redis/hash" : ["update_map", "inc_map", "fields", "strict", "cond"],
                "redis/set" : [],
            },
        },
        "get" : {
            "defaults" : {"fields" : [], "strict" : True},
            "required" : {
                "text/json" : [],
                "text/plain" : [],
                "redis/hash" : ["fields", "strict"],
                "redis/set" : [],
            },
        },
    }

    @classmethod
    def _get_parameters(cls, operation, content_type, **kwargs):
        required_parameters = RedisClient._operation_parameters[operation]["required"][content_type]
        parameters = {}
        for field, value in kwargs.items():
            if field in required_parameters:
                parameters[field] = value
            else:
                raise Exception("unexpected parameter, %s, %s" % (content_type, field))

        for required_parameter in required_parameters:
            if not parameters.has_key(required_parameter):
                parameters[required_parameter] = RedisClient._operation_parameters[operation]["defaults"][required_parameter]

        return parameters

    @reliable_op
    def set(self, data_type, ori_data_key, with_get=False, **kwargs):
        """
        parameters:
            with_get: returns original value if True, default is False
            cond: just set values if cond is met, just support redis/hash now
            nx: if just set value if the key does not exist, just support text/json and text/plain, with_get can't be used with nx
            strict: whether set the non-existed field to none, default is True, just support redis/hash
            redis/hash: fields: get fields, just support redis/hash

        content_types:
            text/json: data is dict object, serialize to text, and override all the values,  with_get == False: returns True, else: returns original value
            text/plain: data is str object, set directly, with_get == False: returns True, else: returns original value
            redis/hash: update_map/inc_map are optional, will update specified fields, with_get == False: returns True if any update is not empty, else: returns original value
            redis/set: data_key is set value, with_get == False, returns True, else returns if existed
        """
        #logging.debug("redis set", data_type, ori_data_key, with_get, kwargs)
        data_key = copy.deepcopy(ori_data_key)
        if not self._enabled:
            return None

        name, content_type, enabled = self._prepare_op(data_type, data_key)
        if not enabled:
            return None

        parameters = RedisClient._get_parameters("set", content_type, **kwargs)

        if content_type == "text/json":
            data = parameters["data"]
            nx = parameters["nx"]

            if data is None:
                raise Exception("data can't be null")
            if not isinstance(data, dict):
                raise Exception("text/json type data before dumps should be dict type")
            data = misc.dumps_jsonx(data)
            if nx and with_get:
                raise Exception("nx can't be used with with_get")

            if not with_get:
                if nx:
                    return self._client.setnx(name, data)
                else:
                    return self._client.set(name, data)
            else:
                data = self._client.getset(name, data)
                return self._load_json_object(data)
        elif content_type == "text/plain":
            data = parameters["data"]
            nx = parameters["nx"]

            if not isinstance(data, str):
                data = encode_string(data)
                if data is None:
                    raise Exception("redis set value must be str, %s, %s, %s" % (data_type, data_key, type(data)))
            if nx and with_get:
                raise Exception("nx can't be used with with_get")

            if not with_get:
                if nx:
                    return self._client.setnx(name, data)
                else:
                    return self._client.set(name, data)
            else:
                return self._client.getset(name, data)
        elif content_type == "redis/hash":
            return self._set_hash(data_type, name, parameters["update_map"], parameters["inc_map"], with_get, parameters["fields"], parameters["strict"], parameters["cond"])
        elif content_type == "redis/set":
            if not isinstance(data_key, str):
                data_key = encode_string(data_key)
                if data_key is None:
                    raise Exception("data_key must be str, %s" % data_key)

            existed = self._client.sadd(name, data_key) == 0
            if not with_get:
                return True
            else:
                return existed
        else:
            raise Exception("unsupported data_type for set, %s" % data_type)

    @reliable_op
    def delete(self, data_type, ori_data_key, *fields):
        """
        text/json: delete the whole row
        text/plain: delete the whole row
        redis/hash: delete specified fields
        redis/set: delete the set member, data_key is the set member
        """
        data_key = copy.deepcopy(ori_data_key)
        if not self._enabled:
            return None

        name, content_type, enabled = self._prepare_op(data_type, data_key)
        if not enabled:
            return None

        if content_type == "text/json" or content_type == "text/plain":
            if len(fields) > 0:
                raise Exception("unexpected fields %s, %s, %s", data_type, data_key, fields)

            return self._client.delete(name)
        elif content_type == "redis/hash":
            if len(fields) == 0:
                return self._client.delete(name)
            else:
                return self._client.hdel(name, *fields)
        elif content_type == "redis/set":
            if len(fields) > 0:
                raise Exception("unexpected fields %s, %s, %s", data_type, data_key, fields)

            if data_key is None:
                return self._client.delete(name)
            elif not isinstance(data_key, str):
                data_key = encode_string(data_key)
                if data_key is None:
                    raise Exception("data_key must be str, %s" % data_key)
            else:
                return self._client.srem(name, data_key)
        else:
            raise Exception("unsupported data_type for delete, %s" % data_type)

    def exists(self, data_type, ori_data_key):
        data_key = copy.deepcopy(ori_data_key)
        if not self._enabled:
            return False

        name, content_type, enabled = self._prepare_op(data_type, data_key)
        return self._client.exists(name)

    def _load_json_object(self, data):
        if data is None:
            return None
        data = misc.loads_jsonx(data)
        if not isinstance(data, dict):
            raise Exception("text/json type data after loads should be dict type")
        return data

    def _get_hash(self, data_type, name, fields, strict):
        data_config = self._data_types.get(data_type)
        if data_config.get("raw", False):
            values = self._client.hgetall(name)
            fields = None
        else:
            values = self._client.hmget(name, fields)

        if values is None:
            return None
        else:
            return self._load_hash_object(values, data_type, name, fields, strict)

    def _load_hash_object(self, values, data_type, name, fields, strict):
        """
        if fields is None, we assume it's raw mode, all fields will be retrieved, and no field encoder/decoder will be used
        """

        data_config = self._data_types.get(data_type)
        input_dict = values if fields is None else dict(zip(fields, values))

        result_dict = {}
        for field, value in input_dict.items():
            if value is None:
                if strict:
                    logging.error("field %s does not exist in the result, %s" % (field, name))
                    #raise Exception("field %s does not exist in the result, %s" % (field, name))
                    return None
                else:
                    value = None
            elif value == "\\None":
                value = None
            else:
                if RedisClient._special_none_pattern.match(value) is not None:
                    value = value[1:]

                if fields is not None:
                    field_configs = filter(lambda field_config : field_config == field if isinstance(field_config, str) else field_config[0] == field, data_config["fields"])
                    if len(field_configs) == 0:
                        raise Exception("unexpected field, %s, %s, %s" % (data_type, name, field))
                    field_config = field_configs[0]
                    if isinstance(field_config, tuple) and field_config[1] != str:
                        decoder = field_config[1]
                        value = decoder(value)

            result_dict[field] = value

        return result_dict

    def _set_hash(self, data_type, name, update_map, inc_map, with_get, fields, strict, cond):
        #refine update_map if not raw
        new_update_map = {}
        data_config = self._data_types.get(data_type)
        raw = data_config.get("raw", False)
        if len(fields) > 0 and raw:
            raise Exception("fields can't be used with raw")

        if raw:
            fields = None

        for field, value in update_map.items():
            if not isinstance(field, str):
                field = encode_string(field)
                if field is None:
                    raise Exception("redis/hash update_map field must be str, %s, %s, %s" % (name, field, value))

            if value is None:
                value = "\\None"
            else:
                if not raw:
                    field_configs = filter(lambda field_config : field_config == field if isinstance(field_config, str) else field_config[0] == field, data_config["fields"])
                    if len(field_configs) == 0:
                        raise Exception("unexpected field, %s, %s, %s" % (data_type, name, field))
                    field_config = field_configs[0]
                    if isinstance(field_config, tuple) and len(field_config) > 2 and field_config[2] != str:
                        encoder = field_config[2]
                        value = encoder(value)

                if isinstance(value, str) and RedisClient._special_none_pattern.match(value) is not None:
                    value = "\\" + value

            if not isinstance(value, str):
                value = str(value)

            new_update_map[field] = value

        update_map = new_update_map

        pipeline = self._client.pipeline(transaction=True)
        if cond is None:
            results = self._execute_set_hash_pipeline(pipeline, name, update_map, inc_map, with_get, fields)
        else:
            results = self._execute_set_hash_cond(pipeline, data_type, name, update_map, inc_map, with_get, fields, strict, cond)

        if results is None:
            return None if with_get else True
        elif results == False:
            return False
        elif len(filter(lambda result : result == False or isinstance(result, Exception), results)) > 0:
            raise Exception("set_hash failed, %s, %s, %s, %s" % (name, update_map, inc_map, results))
        elif with_get:
            values = results[0]
            return self._load_hash_object(values, data_type, name, fields, strict)
        else:
            return True

    def _execute_set_hash_pipeline(self, pipeline, name, update_map, inc_map, with_get, fields):
        """
        if fields is None, we assume it's raw mode, all fields will be retrieved, and no field encoder/decoder will be used
        """

        if with_get:
            if fields is not None:
                pipeline.hmget(name, fields)
            else:
                pipeline.hgetall(name)

        if len(update_map) > 0:
            pipeline.hmset(name, update_map)
        for field, inc_value in inc_map.items():
            if not isinstance(field, str):
                field = encode_string(field)
                if field is None:
                    raise Exception("redis/hash inc_map field name must be str, %s, %s, %s" % (name, field, inc_value))
            elif not isinstance(inc_value, int):
                raise Exception("redis/hash inc_map field value must be int, %s, %s, %s" % (name, field, inc_value))

            pipeline.hincrby(name, field, inc_value)
        if len(pipeline.command_stack) > 0:
            return pipeline.execute()
        else:
            return None

    def _execute_set_hash_cond(self, pipeline, data_type, name, update_map, inc_map, with_get, fields, strict, cond):
        while True:
            try:
                pipeline.watch(name)
                result = self._get_hash(data_type, name, cond["fields"], strict)
                if cond["func"](result):
                    pipeline.multi()
                    return self._execute_set_hash_pipeline(pipeline, name, update_map, inc_map, with_get, fields)
                else:
                    return False
            except redis.exceptions.WatchError:
                continue

    def set_stop_condition(self, stop_condition):
        self._stop_condition = stop_condition
