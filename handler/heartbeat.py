'''
Created on Jul 24, 2012

@author: dhcui
'''

import socket
import datetime
import time
import thread
import multiprocessing
import simplejson
import signal
import smtplib

import twisted.internet.task
import configparser
from ccrawler.utils.log import logging
import ccrawler.db.heartbeatdb as heartbeatdb
import ccrawler.utils.misc as misc
from ccrawler.utils.format import datetime2timestamp
import pexpect
import paramiko

class HeartBeatClient(object):
    def __init__(self, heart_beat_config, handler_key, stop_condition=None):
        self._interval = heart_beat_config["client_interval"]
        self._stop_condition = stop_condition
        self._ip = misc.get_local_ip()
        self._handler_key = handler_key
        self._handler_name = handler_key.split('_')[0]
        self._server_address = heart_beat_config["server_address"]
        self._server_port = heart_beat_config["server_port"]
        self._process_id = multiprocessing.current_process().pid

    def _once(self):
        now = datetime2timestamp(datetime.datetime.utcnow())
        message = {"datetime" : now, "ip" : self._ip, "handler_name" : self._handler_name,
            "pid" : self._process_id, "handler_key" : self._handler_key}

        try:
            self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._client_socket.connect((self._server_address, self._server_port))
            self._client_socket.send(simplejson.dumps(message))
            logging.debug("heartbeat client sent message", message)
        except socket.error as e:
            logging.warn("socket error for heartbeat client", exception = e)
        finally:
            self._client_socket.close()

    def _run(self):
        while not (self._stop_condition is not None and self._stop_condition()):
            time.sleep(self._interval)
            self._once()
        else:
            logging.debug("heartbeat client terminated")

    def start_by_thread(self):
        thread.start_new_thread(self._run, ())
        logging.debug("heartbeat client started")

    def start_by_twisted(self):
        loop = twisted.internet.task.LoopingCall(self._once)
        loop.start(self._interval)

class HeartBeatServer(object):
    '''
    TODO:   1) check if handler count reduced, we need a machine.csv to know handler count per machine; and this server should know the full machine list.
            2) auto restart terminated handler if any, and we need to distinguish normal vs abnormal termination.
    '''

    global_stop_event = multiprocessing.Event()

    @classmethod
    def _stop(cls, signum, frame):
        HeartBeatServer.global_stop_event.set()
        logging.debug("heartbeat server is terminating gracefully")

    def start(self, heart_beat_config):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._server_socket.bind(('', heart_beat_config["server_port"]))
            self._server_socket.listen(heart_beat_config["backlog"])
            self._interval = heart_beat_config["server_interval"]
            self._heart_beat_config = heart_beat_config
            self._last_notification_time = None

            signal.signal(signal.SIGTERM, HeartBeatServer._stop)
            signal.signal(signal.SIGINT, HeartBeatServer._stop) # for ctrl-c

            thread.start_new_thread(self._run, ())

            logging.info("heartbeat server started")
            while not HeartBeatServer.global_stop_event.is_set():
                client_socket, _ = self._server_socket.accept()
                raw_data = client_socket.recv(heart_beat_config["max_data_size"])
                message = simplejson.loads(raw_data)
                logging.debug("heartbeat server received message", message)
                heartbeatdb.save_heartbeat(message)
                client_socket.close()
        except socket.error as e:
            logging.warn("socket error for heartbeat server!!!", exception = e)
        finally:
            self._server_socket.close()
            logging.info("heartbeat server terminated")

    def _run(self):
        while not HeartBeatServer.global_stop_event.is_set():
            time.sleep(self._interval)
            self._check()

    def _check(self):
        heartbeats = heartbeatdb.get_heartbeats(self._heart_beat_config["check_duration"])
        heartbeats = misc.cursor_to_array(heartbeats)
        heartbeats = misc.select(heartbeats, fields=["ip", "handler_name", "pid"])
        heartbeats = misc.distinct(heartbeats, key=str)
	name_count = self.count_by_name(heartbeats)
        config = self.load_cfg(self._heart_beat_config["config_path"])
        results = self.check_detail(config, name_count, config.sections(),self._heart_beat_config["detail_flag"])
        if len(results) > 0:
            result_str = ''
            for result in results:
		result_str += results[result]["text"]
                result_str += '\n'
            self._send_email(
                    self._heart_beat_config["email_server"],
                    self._heart_beat_config["email_from"],
                    self._heart_beat_config["email_tos"],
                    self._heart_beat_config["email_title"],
                    result_str)
            if self._heart_beat_config['repair_flag']:
	        self.repair_service(results, config)
            self._last_notification_time = datetime.datetime.now()

    def _send_email(self, email_server, email_from, email_tos, title, body):
        server = smtplib.SMTP(email_server)
        message = "From: %s\nTo: %s\nSubject: %s\n\n%s" % (email_from, ", ".join(email_tos), title, body)
        server.sendmail(email_from, email_tos, message)
    def count_by_name(self,iterable):
        """
        this function counts the number of service ,and return a dict of result
        the dict's key is handler_name,value of this key is also a dict(dict2),
        dict2's key is server_ip ,value is another dict(dict3),
        dict3 's key is pid,value is the number of its appearance in heartbeat record
        with num of pid of each server's service we can know how many instance of each handler survive in recent time
        """
	def _count(group, item):
            handler_key = str(item['handler_name'])
            ip_key = str(item['ip'])
            pid_key = str(item['pid'])
            if group.has_key(handler_key):
                if group[handler_key].has_key(ip_key):
                    if group[handler_key][ip_key].has_key(pid_key):
                        group[handler_key][ip_key][pid_key] +=1
                    else:
                        group[handler_key][ip_key][pid_key] = 1
                else:
                    group[handler_key][ip_key] = {}
                    group[handler_key][ip_key][pid_key] = 1
            else:
                group[handler_key]={}
                group[handler_key][ip_key] = {}
                group[handler_key][ip_key][pid_key] = 1
            return group

        return reduce(_count, iterable, {})
    def load_cfg(self,filepath):
        """
        this function return the configuration of given filepath
        """
        config = configparser.ConfigParser()
        config.read([filepath])
        return config

    def check_detail(self,config,name_count,sections_list,detail_flag):
        """
        this function check whether each handler's number is same to the number declared in cfg file
        if they are the same,return a empty set,otherwise return the detail difference between the actual
        statistic and the expected one
        """
        def _check_detail(group,item):
            handler_name_dict = self._heart_beat_config["handler_name_dict"]
            item_class_name = None
            item_concurrency_name = None
            item_section_name = None
            for name in handler_name_dict:
                if item.startswith(name):
                    item_class_name = handler_name_dict[name][0]
                    item_concurrency_name = handler_name_dict[name][1]
                    item_section_name = name
                    break
            if item_class_name is not None and item_concurrency_name is not None:
                item_ip = config[item].get('ip')
                item_concurrency = config[item].get(item_concurrency_name,'')
                if len(item_concurrency) == 0:
                    group_id = len(group)+1
                    group[group_id] = {}
                    group[group_id]["text"] = 'cfg file\'s section %s lack the concurrency %s please repair it'%(item_section_name,item_concurrency_name)
                else:
                    item_concurrency = int(item_concurrency)
                    if item_class_name in name_count and item_ip in name_count[item_class_name]:
                        if detail_flag =='detail' and len(name_count[item_class_name][item_ip]) < item_concurrency:
                            group_id = len(group)+1
                            group[group_id] = {}
                            group[group_id]["text"] = 'the handler %s in %s should have %s concurrency but now only %s left'%(item_class_name,item_ip,item_concurrency,len(name_count[item_class_name][item_ip]))
                            group[group_id]["class_name"] = item_class_name
                            group[group_id]["server_ip"] = item_ip
                            group[group_id]["expected_concurrency"] = item_concurrency
                            group[group_id]["actual_concurrency"] = len(name_count[item_class_name][item_ip])

                    elif item_concurrency >0:#if statistic is nothing but cfg expect someone ,add one record
                        group_id = len(group)+1
                        group[group_id] = {}
                        group[group_id]["text"] = 'the handler %s in %s should have %s concurrency but now only %s left'%(item_class_name,item_ip,item_concurrency,0)
                        group[group_id]["class_name"] = item_class_name
                        group[group_id]["server_ip"] = item_ip
                        group[group_id]["expected_concurrency"] = item_concurrency
                        group[group_id]["actual_concurrency"] = 0
                    else:
                        pass
            return group
        return reduce(_check_detail, sections_list,{})

    def repair_service(self,info_dict,config):
        """
        auto repair the dead service
        """
	command_dict = self._heart_beat_config["repair_command"]
        for item_id in info_dict:
            item = info_dict[item_id]
            if "class_name" in item and item['class_name'] in command_dict:
                user = config["DEFAULT"].get("user")
                password = config["DEFAULT"].get("password")
                host = item['server_ip']
                class_name = item['class_name']
                command = command_dict[class_name]
                concurrency_expected = str(item["expected_concurrency"] - item["actual_concurrency"])
                try:
                    self.ssh_cmd(user, host, password, command%concurrency_expected)
                except Exception as e:
                    logging.error(e)
                    #todo: send mail to administrator?

    def ssh_command (self,user, host, password, command):
        """
        This runs a command on the remote host. This could also be done with the
        pxssh class, but this demonstrates what that class does at a simpler level.
        This returns a pexpect.spawn object. This handles the case when you try to
        connect to a new host and ssh asks you if you want to accept the public key
        fingerprint and continue connecting.
        """
        child = pexpect.spawn('ssh -l %s %s %s'%(user, host, command))
        child.sendline(password)
        return child

    def ssh_cmd(self,user, host, password, command):
	"""
	this method can runs a command on the remote host
	better than ssh_command
	need paramiko dependency
	"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, 22, username=user, password=password, timeout=4)
        client.exec_command(command)
        client.close()


    def _check_is_all_crash(self):
        """
        original method of checking whether all service is alive
        not used right now
        """
	heartbeats = heartbeatdb.get_heartbeats(self._heart_beat_config["check_duration"])
        heartbeats = misc.cursor_to_array(heartbeats)
        heartbeats = misc.select(heartbeats, fields=["ip", "handler_name", "pid"])
        heartbeats = misc.distinct(heartbeats)
        handler_counts_per_machine = misc.count(heartbeats, key = lambda heartbeat : "%s_%s" % (heartbeat["ip"], heartbeat["handler_name"]))
        heartbeatdb.save_handler_counts(simplejson.dumps(handler_counts_per_machine), type="handler_counts_per_machine")
        handler_counts = misc.count(heartbeats, key = lambda heartbeat : heartbeat["handler_name"])
        heartbeatdb.save_handler_counts(simplejson.dumps(handler_counts), type="handler_counts_total")
        logging.debug("current alive handler counts", handler_counts)
        #Note: currently we will send email if no handler is running
        if len(filter(lambda handler_name : handler_counts.get(handler_name, 0) == 0, self._heart_beat_config["required_handlers"])) > 0:
            if self._last_notification_time is None or datetime.datetime.now() - self._last_notification_time >= \
                datetime.timedelta(seconds=self._heart_beat_config["notification_duration"]):

                email_body = "some handlers are not running:\n %s" % handler_counts_per_machine
                self._send_email(
                    self._heart_beat_config["email_server"],
                    self._heart_beat_config["email_from"],
                    self._heart_beat_config["email_tos"],
                    self._heart_beat_config["email_title"],
                    email_body)
                self._last_notification_time = datetime.datetime.now()
                logging.error("heartbeat server detects required handlers are not fully running, notification email sent", handler_counts_per_machine)

