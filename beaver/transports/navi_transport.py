# -*- coding: utf-8 -*-

from beaver.transports.base_transport import BaseTransport
from beaver.transports.exception import TransportException

from _navi_mongo import MongoDB
from datetime import datetime

import ujson as json
import time
from _navi_carroute import get_city_mongo_obj
from _navi_bss import BssStorage, make_bss_from_response_body


class NaviTransport(BaseTransport):
    def __init__(self, beaver_config, logger=None):
        super(NaviTransport, self).__init__(beaver_config, logger=logger)
        self._logger.debug("Navi transport {}, {}".format(beaver_config.get('mongo_connection_string'), beaver_config.get('mongo_db')))
        self._logger.debug("Navi transport {}, {}, {}".format(beaver_config.get('bss_url'), beaver_config.get('bss_storage_folder'), beaver_config.get('bss_batch_size')))
        self.connections = {}
        if beaver_config.get('mongo_connection_string') and beaver_config.get('mongo_connection_string') != '':
            self.connections['mongo'] = MongoDB(beaver_config.get('mongo_connection_string'),
                                                beaver_config.get('mongo_db'),
                                                beaver_config.get('mongo_batch_size'),
                                                logger=self._logger)
        if beaver_config.get('bss_url') and beaver_config.get('bss_url') != '':
            self.connections['bss'] = BssStorage(beaver_config.get('bss_url'),
                                                 beaver_config.get('bss_storage_folder'),
                                                 beaver_config.get('bss_batch_size'),
                                                 beaver_config.get('bss_user_agent'),
                                                 logger=self._logger)
        self._bss_ppnot_product = beaver_config.get('bss_ppnot_product')
        self.lines_counter = 0
        self._logger.info("Navi transport connections: {}".format(self.connections.keys()))
        self._connect()

    def callback(self, filename, lines, **kwargs):
        """publishes lines one by one to the given topic"""
        timestamp = self.get_timestamp(**kwargs)
        self.lines_counter = self.lines_counter + len(lines)
        self._logger.debug("Callback lines counter " + str(self.lines_counter))
        if kwargs.get('timestamp', False):
            del kwargs['timestamp']
        for line in lines:   
            if 'mongo' in self.connections and 'POST /carrouting/3' in line and 'driving_direction' in line:
                self._insert_carrouting(line, self.connections['mongo'])

            if 'bss' in self.connections and 'POST /ctx/' in line:
                self._insert_ctx(line, self.connections['bss'])
        
    def _insert_ctx(self, line, inserter=None):
        try:
            self._logger.debug("Processing BSS")
            document = line.decode('string_escape')
            obj = json.loads(document)
            if 'response_body' not in obj:
                self._logger.debug("No response body in BSS " + str(obj))
                return  
            req = str(obj["request"])
            moses_ver = (req.split(" ")[1]).split("/")[2]          
            bss_messages = make_bss_from_response_body(obj['response_body'], moses_ver, self._bss_ppnot_product)
            self._logger.debug("BSS messages " + str(len(bss_messages)))
            inserter.insert(bss_messages)            
        except Exception as ex:
            self._logger.error('Exception: {}\n'.format(str(ex)) + document)

    def _insert_carrouting(self, line, inserter):
        try:
            start_time = time.time()
            document = line.decode('string_escape')
            obj = json.loads(document)
            self._logger.debug("Callback for carrouting json parse: {0:.6f}".format(time.time() - start_time))
            
            start_time = time.time()
            city, calculated_obj = get_city_mongo_obj(obj)
            self._logger.debug("Callback for carrouting calc Carrouting: {0:.6f}".format(time.time() - start_time))
            self._logger.debug("Carrouting processed msg: " + calculated_obj['time_local'])

            start_time = time.time()
            inserter.insert(city, calculated_obj)
            self._logger.debug("Callback for carrouting mongo insert: {0:.6f}".format(time.time() - start_time))
            
        except Exception as ex:
            self._logger.error('Exception: {} {}\n'.format(str(ex.message), line))

    def _connect(self):
        for connection in self.connections.values():
            connection._connect()

    def reconnect(self):
        for connection in self.connections.values():
            connection._connect()

    def unhandled(self):
        return True
