#!/usr/bin/env python
# -*- coding: utf-8 -*-
import math
import shapely
import ujson as json
from shapely.geometry import Point, LineString

def get_value(data, key, default):
	if key in data:
		return data[key]
	else:
		return default

def toWGS84(xLon, yLat):
	# Check if coordinate out of range for Latitude/Longitude
	if (abs(xLon) < 180) and (abs(yLat) > 90):
		return

	# Check if coordinate out of range for Web Mercator
	# 20037508.3427892 is full extent of Web Mercator
	if (abs(xLon) > 20037508.3427892) or (abs(yLat) > 20037508.3427892):
		return

	semimajorAxis = 6378137.0  # WGS84 spheriod semimajor axis

	latitude = (1.5707963267948966 - (2.0 * math.atan(math.exp((-1.0 * yLat) / semimajorAxis)))) * (180 / math.pi)
	longitude = ((xLon / semimajorAxis) * 57.295779513082323) - (
	(math.floor((((xLon / semimajorAxis) * 57.295779513082323) + 180.0) / 360.0)) * 360.0)

	return [longitude, latitude]

def toWebMercator(xLon, yLat):
	# Check if coordinate out of range for Latitude/Longitude
	if (abs(xLon) > 180) and (abs(yLat) > 90):
		return

	semimajorAxis = 6378137.0  # WGS84 spheriod semimajor axis
	east = xLon * 0.017453292519943295
	north = yLat * 0.017453292519943295

	northing = 3189068.5 * math.log((1.0 + math.sin(north)) / (1.0 - math.sin(north)))
	easting = semimajorAxis * east

	return [easting, northing]

def toMercator(wgs_pnts):
	mercator_pnts = []
	for pnt in wgs_pnts:
		mercator_pnts.append(toWebMercator(pnt[0], pnt[1]))
	return mercator_pnts

def toWgs(mercator_pnts):
	wgs_pnts = []
	for pnt in mercator_pnts:
		wgs_pnts.append(toWGS84(float(pnt[0]), float(pnt[1])))
	return wgs_pnts

def cut(line, distance):
	# Cuts a line in two at a distance from its starting point
	# This is taken from shapely manual
	if distance <= 0.0 or distance >= line.length:
		return [LineString(), LineString(line)]
	coords = list(line.coords)
	for i, p in enumerate(coords):
		pd = line.project(Point(p))
		if pd == distance:
			return [
				LineString(coords[:i+1]),
				LineString(coords[i:])]
		if pd > distance:
			cp = line.interpolate(distance)
			return [
				LineString(coords[:i] + [(cp.x, cp.y)]),
				LineString([(cp.x, cp.y)] + coords[i:])]

def line_substring(line, part_from, part_to):
	pnt_from = line.interpolate(part_from)
	pnt_to = line.interpolate(part_to)

	lines = split_line_with_points(line, [pnt_from, pnt_to])
	return lines[1]

def split_line_with_points(line, points):
	"""Splits a line string in several segments considering a list of points.

	The points used to cut the line are assumed to be in the line string
	and given in the order of appearance they have in the line string.

	>>> line = LineString( [(1,2), (8,7), (4,5), (2,4), (4,7), (8,5), (9,18),
	...        (1,2),(12,7),(4,5),(6,5),(4,9)] )
	>>> points = [Point(2,4), Point(9,18), Point(6,5)]
	>>> [str(s) for s in split_line_with_points(line, points)]
	['LINESTRING (1 2, 8 7, 4 5, 2 4)', 'LINESTRING (2 4, 4 7, 8 5, 9 18)', 'LINESTRING (9 18, 1 2, 12 7, 4 5, 6 5)', 'LINESTRING (6 5, 4 9)']

	"""
	segments = []
	current_line = line
	for p in points:
		d = current_line.project(p)
		seg, current_line = cut(current_line, d)
		segments.append(seg)
	segments.append(current_line)
	return segments

class QueryPoint(object):
	def __init__(self, data):
		self.type = data["type"]
		self.x = float(data["x"])
		self.y = float(data["y"])
		self.zlevel = data.get("zlevel", 0)

class WayPoint(object):
	def __init__(self, data):
		self.id = data["id"]
		self.part = float(data["part"])
		self.seconds = float(data["seconds"])
		self.meters = data.get("meters", 0)

class CarQuery(object):
	def __init__(self, data):
		self.locale = data.get("locale", '')
		self.type = data.get("type", '')
		self.point_a_name = data.get("point_a_name", '')
		self.point_b_name = data.get("point_b_name", '')
		self.points = [QueryPoint(point) for point in data['points']]

class Car_Edge(object):
	def __init__(self, data=None):
		if data is None:
			return
		self.id = int(data["id"])
		self.class_id = data.get('class', 0)
		self.begin_part = float(data["begin_part"])
		self.end_part = float(data["end_part"])
		self.length = data.get('length', 0)
		self.shape_id = int(data["shape_id"])
		self.speed = data.get('speed', 0)
		self.width = data.get('width', 0)
		self.traffic_type = data.get('traffic_type', 0)
		self.time = data.get('time', 0)
		self.default_speed = data.get('default_speed', 0)
		self.street_id = data.get('street_id', 0)

		self.segment = {}
		self.segment['begin'] = 0.0
		self.segment['end'] = 1.0
		if "segment" in data:
			self.segment['begin'] = float(data["segment"]["begin"])
			self.segment['end'] = float(data["segment"]["end"])

		self.z_level_begin = data.get("z_level_begin", 0)
		self.z_level_end = data.get("z_level_end", 0)
		self.geometry = []
		if "geometry" in data:
			self.geometry = data["geometry"]

	def copy_and_init(self, data):
		newInstance = Car_Edge()
		for k, v in self.__dict__.items():
			if k == 'segment':
				newInstance.segment = dict()
				if 'segment' in data:
					newInstance.segment['begin'] = float(data["segment"]["begin"])
					newInstance.segment['end'] = float(data["segment"]["end"])
				else:
					newInstance.segment['begin'] = self.segment['begin']
					newInstance.segment['end'] = self.segment['end']
				continue
			if k == 'geometry':
				if "geometry" in data:
					newInstance.geometry = data["geometry"]
				else:
					newInstance.__setattr__(k,list(v))
				continue
			newInstance.__setattr__(k, v)		
		return newInstance

	def path(self):
		geometry = []
		if self.length:
			if self.segment['begin'] != 0.0 or self.segment['end'] != 1.0:
				if self.segment['begin'] != self.segment['end']:
					# convert to Mercator
					wgs_pnts = []
					for pnt in self.geometry:
						pnts = pnt.split(' ')
						wgs_pnts.append((float(pnts[0]), float(pnts[1])))

					mercator_pnts = toMercator(wgs_pnts)
					# cut part of segment
					line_part = line_substring(shapely.geometry.LineString(mercator_pnts), self.segment['begin'] * self.length, self.segment['end'] * self.length)
					# convert to WGS84
					wgs_pnts = toWgs(shapely.geometry.base.dump_coords(line_part))
					for pnt in wgs_pnts:
						geometry.append(str(pnt[0]) + ' ' + str(pnt[1]))
			else:
				geometry = geometry + self.geometry
		return geometry


class CarDD_Intruction(object):
	def __init__(self, data):
		self.id = int(data["id"])
		self.type = data["type"]
		self.names = data["names"]


class CarDD_Item(object):
	def __init__(self, data, subroutes):
		self.id = int(data["id"])
		self.distance = int(data["distance"])
		self.duration = int(data["duration"])
		self.subroute_id = int(data["subroute"])
		self.subroute = subroutes[self.subroute_id]
		self.edges = []
		for edge in data["edges"]:
			edge_id = int(edge["id"])
			if edge_id in self.subroute.edges:
				sub_edge = self.subroute.edges[edge_id].copy_and_init(edge)
			else:
				sub_edge = Car_Edge(edge)
			self.edges.append(sub_edge)

		self.instructions = CarDD_Intruction(data["instruction"])

	def path(self):
		geometry = []
		for edge in self.edges:
			for item in edge.path():
				geometry.append(item)
		return geometry

class CarDD_Subroute(object):
	def __init__(self, data):
		self.distance = int(data["distance"])
		self.duration = int(data["duration"])
		self.id = int(data["id"])
		self.start_point = WayPoint(data["start_point"])
		self.finish_point = WayPoint(data["finish_point"])

		self.edges = {}
		edges = data["edges"]
		for edge in edges[0]:
			self.edges[int(edge["id"])] = Car_Edge(edge)

class CarDD(object):
	def __init__(self, data):
		self.type = data["type"]
		self.id = data["id"]
		self.rule = data["rule"]

		self.subroutes = {}
		for subroute in data["subroutes"]:
			self.subroutes[int(subroute["id"])] = CarDD_Subroute(subroute)

		self.items = []
		self.items = [CarDD_Item(item, self.subroutes) for item in data["items"]]

	def path(self):
		geometry = []
		for item in self.items:
			geometry = geometry + item.path()
		return geometry


class CarRoute(object):
	def __init__(self, data):
		self.dd = CarDD(data["driving_direction"])
		self.route_id = data["route_id"]

	def path(self):
		wkt_line = ','.join(self.dd.path())
		return 'LINESTRING({0})'.format(wkt_line)

class CarRoutes(object):
	def __init__(self, data):
		if not data:
			return
		if isinstance(data, list):
			data = data[0]
		if 'carrouting' in data:
			data = data['carrouting']
		if "query" in data:
			self.query = CarQuery(data["query"])
		self.user_id = data.get("user_id", '')
		self.routes = []
		if len(data["result"]) != 0:
			self.routes = [CarRoute(data["result"][0])]		

	def __iter__(self):
		return (x for x in self.routes)


def calculate_stats(jsobj):
	distance = 0				# meters
	duration = 0			    # seconds
	average_speed = 0
	left_turns = 0
	right_turns = 0
	streets_count = 0
	humps_count = 0
	cameras_count = 0

	if 'response_body' not in jsobj:
		return
	if 'carrouting' in jsobj['response_body']:
		response = jsobj['response_body']['carrouting']['result'][0]
	else:
		response = jsobj['response_body']['result'][0]
	try:
		distance = int(response['driving_direction']['subroutes'][0]['distance'])
		duration = int(response['driving_direction']['subroutes'][0]['duration'])
		_streets = response['driving_direction']['subroutes'][0]['streets']
		if _streets:
			streets_count = len(_streets)
		else:
			streets_count = 0

		for item in response['driving_direction']['items']:
			if 'instruction' in item:
				instructions = key_check(item, 'instruction')
				if 'turn_direction' in instructions:
					turn_string = instructions['turn_direction']
					if 'left' in turn_string:
						left_turns += 1
					if 'right' in turn_string:
						right_turns += 1
		for edge in response['driving_direction']['subroutes'][0]['edges'][0]:
			if 'signs' in edge:
				signs = key_check(edge, 'signs')
				for sign in signs:
					if 'hump' in sign:
						humps_count += 1
			if 'cameras' in edge:
				cameras_count += len(edge['cameras'])
	except Exception as e:
		raise e

	average_speed = (distance / duration) * 3.6

	jsobj["distance"] = int(distance)
	jsobj["duration"] = int(duration)
	jsobj["average_speed"] = int(average_speed)
	jsobj["left_turns"] = left_turns
	jsobj["rigth_turns"] = right_turns
	jsobj["streets_count"] = streets_count
	jsobj["humps_count"] = humps_count
	jsobj["cameras_count"] = cameras_count
	return jsobj


def key_check(jsobj, key):
	try:
		result = json.loads(jsobj[str(key)])
	except TypeError:
		result = jsobj[str(key)]
	return result

def get_city_mongo_obj(line_obj):
	req = str(line_obj["request"])
	city = (req.split(" ")[1]).split("/")[3]
	car_routes = CarRoutes(line_obj['response_body'])
	calculated_obj = calculate_stats(line_obj)
	calculated_obj["geometry"] = car_routes.routes[0].path()
	calculated_obj["user_id"] = car_routes.user_id
	calculated_obj["route_id"] = car_routes.routes[0].route_id
	return city, calculated_obj
