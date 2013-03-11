'''
Created on Jul 31, 2012

@author: dhcui
'''

import hashlib
import pymongo

class MqAuxStore(object):
    def __init__(self, host, port, name):
        connection = pymongo.Connection(host, port)
        self._db = connection[name]
        indexes = [("message_type", 1), ("message_id", 1), ("expires", 1)]
        self._db["expiredMessages"].ensure_index(indexes)

    def check_message_expires(self, message_type, message_id, timestamp):
        cond = {"message_type" : message_type, "message_id" : message_id, "expires" : {"$gt" : timestamp}}
        message = self._db.expiredMessages.find_one(cond)
        if message is not None:
            self._db.expiredMessages.remove(cond)
            return True
        else:
            return False

    def add_expired_message(self, message_type, message_id, timestamp):
        key = ''.join([message_type, message_id, str(timestamp)])
        row_id = hashlib.md5(key).hexdigest()
        self._db.expiredMessages.save({"_id" : row_id, "message_type" : message_type, "message_id" : message_id, "expires" : timestamp})