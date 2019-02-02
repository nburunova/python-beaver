#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import gzip 
import ujson as json
import uuid
from time import time
from os import path, mkdir, remove
from datetime import datetime
from beaver.transports.exception import TransportException


def del_none(d):
    for key, value in list(d.items()):
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
        elif isinstance(value, list):
            dicts = [el for el in value if isinstance(el, dict)]
            for subd in dicts:
                del_none(subd)

def make_payload(data):
    """
    data (json)
    """
    d = {
        'id': data['id'],
        'route_id': data.get('route_id', None),
        'total_duration': data.get('total_duration', None),
        'ui_total_duration': data.get('ui_total_duration', None),
        'ui_pedestrian_info': data.get('ui_pedestrian_info', None),
        'transfer_count': data['transfer_count'],
        'crossing_count': data['crossing_count'],
        'pedestrian': data.get('pedestrian', False),
        'waypoints': [
            {
                'subtype': waypoint['subtype'],
                'routes': waypoint['routes'],
                'substrate': waypoint['substrate'],
                'combined': waypoint['combined']
            } for waypoint in data['waypoints']
        ] if 'waypoints' in data else None,
        'movements': [
            {
                'id': movement['id'],
                'type': movement['type'],
                'incoming_line': {
                    'type': movement['incoming_line']['type']
                } if 'incoming_line' in movement else None,
                'outcoming_line': {
                    'type': movement['outcoming_line']['type']
                } if 'outcoming_line' in movement else None,
                'routes_groups': [
                    {
                        'geometry_id': r_group['geometry_id'],
                        'metro': {
                            'line_name': r_group['metro']['line_name'],
                            'boarding_suggest': r_group['metro'].get('boarding_suggest', None),
                            'boarding_parts_suggest': r_group['metro'].get('boarding_parts_suggest', None)
                        } if 'metro' in r_group else None,
                        'platforms': {
                            'names': r_group['platforms'].get('names', None)
                        } if 'platforms' in r_group else None,
                        'routes': [
                            {
                                'subtype': route['subtype'],
                                'subtype_name': route['subtype_name'],
                                'names': route.get('names', None),
                                'schedules': [
                                    {
                                        'type': schedule['type'],
                                        'start_time': schedule['start_time'],
                                        'precise_time': schedule.get('precise_time', None),
                                        'period': schedule.get('period', None)
                                    } for schedule in route['schedules']
                                ] if 'schedules' in route else None,
                                'schedules_events': [
                                    {
                                        'type': schedule_event['type'],
                                        'precise_time': schedule_event.get('precise_time', None),
                                        'start_time': schedule_event['start_time']
                                    } for schedule_event in route['schedules_events']
                                ] if 'schedules_events' in route else None
                            } for route in r_group['routes']
                        ] if 'routes' in r_group else None
                    } for r_group in movement['routes_groups']
                ] if 'routes_groups' in movement else None,
                'waypoint': {
                    'subtype': movement['waypoint']['subtype'],
                    'name': movement['waypoint']['name'],
                    'combined': movement['waypoint'].get('combined', None),
                    'navigation': 
                    {
                        'from': movement['waypoint']['navigation'].get('from', None),
                        'from_name': movement['waypoint']['navigation']['from_name'],
                        'to': movement['waypoint']['navigation'].get('to', None),
                        'to_name': movement['waypoint']['navigation'].get('to_name', None),
                    }
                },
                'alternatives': [
                    {
                        'layer_code': alternative.get('layer_code', None),
                        'geometry': [
                            {
                                'selection': geom['selection'],
                                'z_first': geom.get('z_first', None),
                                'z_last': geom.get('z_last', None)
                            } for geom in alternative['geometry']
                        ],
                        'platforms_groups': [
                            {
                                'id': plat_group['id'],
                                'platforms': [
                                    {
                                        'id': plat['id'],
                                        'uid': plat['uid'],
                                        'geometry': plat['geometry']
                                    } for plat in plat_group['platforms']
                                ]
                            } for plat_group in alternative['platforms_groups']
                        ] if 'platforms_groups' in alternative else None,
                        'platforms': [
                            {
                                'id': platform['id'],
                                'uid': platform['uid'],
                                'geometry': platform['geometry']
                            } for platform in alternative['platforms']
                        ] if 'platforms' in alternative else None,
                        'entrances': [
                            {
                                'geometry': entrance['geometry'],
                                'name': entrance['name']
                            } for entrance in alternative['entrances']
                        ] if 'entrances' in alternative else None
                    } for alternative in movement['alternatives']
                ] if 'alternatives' in movement else None
            } for movement in data['movements']
        ]
    }
    del_none(d)
    return d

def prepare_bss_message(raw_payload, moses_version, product):
    """
    raw_msg (dict)
    """
    try:
        filtered_payload = make_payload(raw_payload)
        return {
           "type": 402,
            "payload": filtered_payload,
            "eventId": str(uuid.uuid1()),
            "timestamp": int(time()*1000),
            "common": {
                "formatVersion": 3,
                "appVersion": str(moses_version),
                "product": int(product)
            }
        }
    except Exception as e:
        raise TransportException(e.message)

def make_bss_from_response_body(resp_body, moses_ver, product):
    bss = []
    for o in resp_body:
        if isinstance(o, dict):
            bss.append(prepare_bss_message(o, moses_ver, product=product))
        elif isinstance(o, list):
            bss = bss + make_bss_from_response_body(o, moses_ver, product=product)
    if len(bss) != 0:
        return bss


class BssStorage():
    def __init__(self, url, storage_folder, batch_size, user_agent, logger=None):
        self._logger = logger
        self._url = url
        self._batch_size = batch_size
        self._batch = []
        if not path.exists(storage_folder):
            mkdir(storage_folder)
        self._storage_folder = storage_folder if storage_folder !='' else None
        self._headers = {
            'Content-Encoding': 'gzip',
            'Content-Type': 'application/json',
            'User-Agent': user_agent
        }
        self._logger.info("Cretaed BSS sender: url {}, storage folder {}, batch size {}".format(self._url, self._storage_folder, self._batch_size))

    def insert(self, bss_msgs):
        """
        bss_msgs (list)
        """
        self._batch = self._batch + bss_msgs
        self._logger.debug("Added BSS msg, total bs n batch " + str(len(self._batch)))
        if len(self._batch) < self._batch_size:            
            return
        self.send_batch()

    def send_batch(self):
        try:
            filename = path.join(self._storage_folder, '{}.gz'.format(datetime.now().time()))
            with gzip.open(filename, 'wb') as bss_arch:
                json.dump(self._batch, bss_arch)
                self._logger.info("Saved BSS batch to file " + str(filename))
            with open(filename) as post_data:
                r = requests.post(self._url, data=post_data.read(), headers=self._headers)
                if r.status_code != 200:
                    remove(filename)
                    raise TransportException("Send to BSS server error: {}, {}".format(r.status_code, r.content))
                self._logger.info("Sent BSS batch to file " + str(filename))
            self._batch = []
            self._logger.debug("Empty BSS batch")
            remove(filename)
        except OSError as e:
            self._logger.warning(e.message)
        except Exception as e:
            raise TransportException(e.message)            

    def _connect(self):
        return True

    def reconnect(self):
        self._connect()
    
    def interrupt(self):
        self.send_batch()

    def unhandled(self):
        self.send_batch()
