# -*- coding: utf-8 -*-
from pymongo import MongoClient as _mongo_client

from beaver.transports.base_transport import BaseTransport
from beaver.transports.exception import TransportException
from datetime import datetime


class Bulk():
    def __init__(self, db, logger=None):
        self.db = db
        self._bulks = {}
        self.size = 0
        self._logger = logger

    def add(self, city, doc):
        if city not in self._bulks:
            self._bulks[city] = self.db.get_collection(city).initialize_ordered_bulk_op()
        self._bulks[city].insert(doc)
        self.size += 1
        self._logger.debug("Mongo total lines in batch" + str(self.size))

    def fulsh(self):
        try:
            for city, bulk in self._bulks.iteritems():
                bulk.execute()
                self._logger.info("Mongo: Inserted bulk for city: " + city)
            self._bulks = {}
            self.size = 0
        except Exception as e:
            raise TransportException(e.message)


class MongoDB():
    def __init__(self, conn_str, db_name, batch_size=10, logger=None):
        self.obj_counter = 0
        self._logger = logger
        self.connection_string = conn_str
        self.db_name = db_name
        self.bulk = None
        self.batch_size = batch_size
        self._connect()
        self._logger.info("Mongo client created, batch size " + str(self.batch_size))

    def insert(self, city, navi_obj):
        self.bulk.add(city, navi_obj)
        if self.bulk.size >= self.batch_size:
            self.bulk.fulsh()

    def _connect(self):
        try:
            self._logger.info("Connect to Mongo")
            client = _mongo_client(self.connection_string)
            db = client[self.db_name]
            self.db = db
            if self.bulk:
                self._logger.debug("Mongo: Bulk not empty")
                self.bulk.fulsh()
            self.bulk = Bulk(self.db, self._logger)
            self._logger.debug("Mongo: Bulk created")
        except Exception, e:
            raise TransportException("Mongo: {} - {}".format(datetime.now(), e.message))

    def reconnect(self):
            self._connect()
    
    def interrupt(self):
        if self.bulk:
            self._logger.debug("Mongo: interrupt flush")
            self.bulk.fulsh()

    def unhandled(self):
        if self.bulk:
            self._logger.debug("Mongo: unhandled flush")
            self.bulk.fulsh()
        return True