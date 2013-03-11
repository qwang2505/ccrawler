'''
Created on June, 27, 2012

@author: dhcui
'''

import thread
import time
import unittest
import multiprocessing
import sys
import datetime

import ccrawler.messagequeue.rabbitmq_blocking_client as rabbitmq_blocking_client
import ccrawler.common.settings as common_settings

class RabbitMQBlockingClientTest(unittest.TestCase):

    def _wrapper(self, key, message_config, func):
        message_configs = {}
        message_configs["__default_message_config"] = common_settings.message_configs["__default_message_config"]
        message_configs[key] = message_config
        client = rabbitmq_blocking_client.RabbitMQBlockingClient(common_settings.mq_client_config, message_configs)

        try:
            func(client, key)
        except:
            raise
        finally:
            client.dispose(True)

    def test_default(self):
        def func(client, key):
            # test publish/get/ack
            client.publish(key, "hello world")
            ret = client.get(key)
            print ret
            client.ack(key, ret["__delivery_tag"])
            self.assertTrue(ret is not None)

            # test empty get
            ret = client.get(key, wait_secs = 0)
            self.assertTrue(ret is None)

        self._wrapper("test1", {"content_type" : "binary"}, func)

    def test_priority_queue(self):

        def func(client, key):
            # test publish/get/ack
            client.publish(key, "e", priority = 3)
            client.publish(key, "a", priority = 2)
            client.publish(key, "b", priority = 1)
            client.publish(key, "c", priority = 0)
            client.publish(key, "d", priority = 1)
            try:
                client.publish(key, "x", priority = 5)
                self.assertTrue(False)
            except:
                pass

            self.assertTrue(client.get(key)["__body"] == "c")
            self.assertTrue(client.get(key)["__body"] == "b")
            self.assertTrue(client.get(key)["__body"] == "d")
            self.assertTrue(client.get(key)["__body"] == "a")
            self.assertTrue(client.get(key)["__body"] == "e")
            self.assertTrue(client.get(key)["__body"] == "x")

        self._wrapper("test2", {"priority_level" : 4, "auto_ack" : True, "content_type" : "binary"}, func)
    def test_get_wait(self):
        def func(client, key):
            # test publish/get/ack
            client.publish(key, "e", priority = 3)
            self.assertTrue(client.get(key, 5)["__body"] == 'e')
            self.assertTrue(client.get(key, 5) is None)
        self._wrapper("test3", {"priority_level" : 4, "auto_ack" : True, "content_type" : "binary"}, func)

    def test_serialize(self):
        def func(client, key):
            # test publish/get/ack
            message = {"url" : "xyz", "priority" : 1}
            client.publish(key, message, priority = 0)
            ret_message = client.get(key)
            del ret_message["__delivery_tag"]
            self.assertTrue(message == ret_message)
        self._wrapper("test4", {"priority_level" : 4, "auto_ack" : True, "content_type" : "text/json"}, func)

    def test_create_client(self):
        common_settings.mq_client_config["parameters"]["lazy_load"] = False
        try:
            common_settings.load_global_mq_client(True)
        except:
            raise
        finally:
            common_settings.mq_client_config["parameters"]["lazy_load"] = True

    def test_ttl(self):
        def func(client, key):
            client.publish(key, {"msg" : "hello world"})
            self.assertTrue(client.get(key, wait_secs=0) is not None)

            client.publish(key, {"msg" : "hello world"})
            time.sleep(6)
            self.assertTrue(client.get(key, wait_secs=0) is None)
        self._wrapper("test5", {"auto_ack" : True, "content_type" : "text/json", "x_message_ttl" : 5000}, func)

    def test_timestamp_expires_false(self):
        def func(client, key):
            client.publish(key, {"url" : "a0", "value" : "xyz"})
            self.assertTrue(client.get(key, wait_secs=0) is not None)

        self._wrapper("test6", {"message_ids" : ["url"], "with_timestamp" : True, "timestamp_expires" : True, "auto_ack" : True, "content_type" : "text/json"}, func)

    def test_timestamp_expires_true(self):
        def func(client, key):
            message = {"url" : "a0", "value" : "xyz"}
            client.publish(key, message)
            client.expire_message(key, message)
            self.assertTrue(client.get(key, wait_secs=0) is None)

            message = {"url" : "a0", "value" : "xyz1"}
            client.publish(key, message)
            client.expire_message(key, message)
            client.publish(key, {"url" : "a0", "value" : "xyz2"})
            res = client.get(key, wait_secs=0)
            del res["__delivery_tag"]
            del res["__timestamp"]
            self.assertTrue(res == {"url" : "a0", "value" : "xyz2"})

        common_settings.mq_client_config["aux_store"]["enabled"] = True
        self._wrapper("test7", {"message_ids" : ["url"], "with_timestamp" : True, "timestamp_expires" : True, "auto_ack" : True, "content_type" : "text/json"}, func)
        common_settings.mq_client_config["aux_store"]["enabled"] = False

    def test_timestamp_expires_no_aux_store(self):
        def func(client, key):
            message = {"url" : "a0", "value" : "xyz"}
            client.publish(key, message)
            client.expire_message(key, message)
            self.assertTrue(client.get(key, wait_secs=0) is not None)

        common_settings.mq_client_config["aux_store"]["enabled"] = False
        self._wrapper("test8", {"message_ids" : ["url"], "with_timestamp" : True, "timestamp_expires" : True, "auto_ack" : True, "content_type" : "text/json"}, func)
        common_settings.mq_client_config["aux_store"]["enabled"] = False

    def _message_validation(self, message, expected):
        del message["__delivery_tag"]
        del message["__timestamp"]
        self.assertEqual(message, expected)

    def test_politeness(self):
        def func(client, key):
            message = {"url" : "a0"}
            message["__group_hash"] = "b" #hash("b") % 2 = 1
            client.publish(key, message, priority = 1)
            client.publish(key, {"url" : "a1", "__group_hash" : "a"}, priority = 1) #hash("a") % 2 = 0
            client.publish(key, {"url" : "a2", "__group_hash" : "a"}, priority = 1) #hash("a") % 2 = 0

            self._message_validation(client.get(key), {"url" : "a1"})
            self._message_validation(client.get(key), {"url" : "a0"})
            self._message_validation(client.get(key), {"url" : "a2"})
        self._wrapper("test9", {"message_ids" : ["url"], "with_timestamp" : True,
            "timestamp_expires" : False, "auto_ack" : True, "content_type" : "text/json",
            "group_mode" : True, "priority_level" : 2, "group_counts" : [1, 2]}, func)

    def test_rpc(self):
        def server(client, key):
            while True:
                message = client.get(key, wait_secs=-1)
                print message
                if message["url"] == "timeout":
                    time.sleep(3)

                if message["url"] == "failure":
                    result = ""
                else:
                    result = "returned " + message["url"]

                client.reply(key, message, result)
                if message["url"] == "$terminate":
                    break
        def validate(client, key, url):
            message = {"url" : url}
            print message
            result = client.rpc(key, message)
            if url == "timeout":
                expected = None
            elif url == "failure":
                expected = ""
            else:
                expected = "returned " + message["url"]

            self.assertEqual(result, expected)

        def validates(client, key, prefix, count):
            for i in range(count):
                if i % 10 == 0:
                    url = "timeout"
                elif i % 9 == 0:
                    url = "failure"
                else:
                    url = prefix + str(i)
                validate(client, key, url)

        def func(client, key):
            #p = multiprocessing.Process(target=server, args=(client,key))
            #p.start()
            thread.start_new_thread(server, (client, key))
            validate(client, key, "a0")
            validate(client, key, "a1")
            validate(client, key, "a2")

            thread.start_new_thread(validates, (client, key, "first", 100))
            thread.start_new_thread(validates, (client, key, "second", 100))
            thread.start_new_thread(validates, (client, key, "third", 100))

            validate(client, key, "$terminate")

        self._wrapper("test10", {"auto_ack" : True, "timeout" : 5, "rpc_reply_content_type" : "binary"}, func)

class TestRpc(object):
    def _wrapper(self, key, message_config, func, dispose=False):
        message_configs = {}
        message_configs["__default_message_config"] = common_settings.message_configs["__default_message_config"]
        message_configs[key] = message_config
        client = rabbitmq_blocking_client.RabbitMQBlockingClient(common_settings.mq_client_config, message_configs)

        try:
            func(client, key)
        except:
            raise
        finally:
            if dispose:
                client.dispose(True)

    def test_rpc_client(self, prefix, count):
        def validate(client, key, url):
            message = {"url" : url}
            print message
            result = client.rpc(key, message)
            if url == "timeout":
                expected = None
            elif url == "failure":
                expected = ""
            else:
                expected = "returned " + message["url"]

            if result != expected:
                print "result != expected, %s, %s" % (result, expected)
            else:
                print url, "succeeded"

        def validates(client, key, prefix, count):
            start_time = datetime.datetime.utcnow()
            for i in range(count):
                if i % 51 == 0:
                    url = "timeout"
                elif i % 5 == 0:
                    url = "failure"
                else:
                    url = prefix + str(i)
                validate(client, key, url)
            end_time = datetime.datetime.utcnow()
            print end_time - start_time

        def func(client, key):
            validate(client, key, "a0")
            validate(client, key, "a1")
            validate(client, key, "a2")

            validates(client, key, prefix, int(count))

        self._wrapper("test10", {"auto_ack" : True, "timeout" : 5, "rpc_reply_content_type" : "binary"}, func)

    def test_rpc_server(self):
        def server(client, key):
            while True:
                message = client.get(key, wait_secs=-1, sleep_secs = 0)
                print message
                if message["url"] == "timeout":
                    time.sleep(6)

                if message["url"] == "failure":
                    result = ""
                else:
                    result = "returned " + message["url"]

                client.reply(key, message, result)
                if message["url"] == "$terminate":
                    break

        def func(client, key):
            #p = multiprocessing.Process(target=server, args=(client,key))
            #p.start()
            server(client, key)

        self._wrapper("test10", {"auto_ack" : True, "timeout" : 200, "rpc_reply_content_type" : "binary"}, func)

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        unittest.main()
    elif sys.argv[1] == "server":
        test = TestRpc()
        test.test_rpc_server()
    elif sys.argv[1] == "client":
        test = TestRpc()
        test.test_rpc_client(sys.argv[2], sys.argv[3])
