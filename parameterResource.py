from functions_db import db
import functions_stuff as stuff
from datetime import datetime, timedelta, date, time
from functions_config import config

class parameterResource:
	def __init__(self, param_id):
		self.param_id = param_id
		self.metadata = None
		self.placeType = None
		self.place_id = None
		self.parameterType = None
		self.edescription = ''
		self.connectionActive = False
		self.newestArchiveTime = None
		self.initCompleted = self.initialize()

	def checkParameterExists(self):
		query = ' SELECT prp_id FROM "Tepl"."ParamResPlc_cnt" WHERE prp_id = $param_id '
		args = {'param_id': self.param_id}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result:
			return True
		return False

	def setParameterMetadata(self):
		query = '\
		SELECT 	paramlist.prp_id as _paramId_, \
				paramlist."ParamRes_id" as _paramTypeId, \
				paramres."Name" as _paramName, \
				place.plc_id as _placeId, \
				place."Name" as _placeName, \
				place.typ_id as _placeTypeId, \
				placetype."Name" as _placeTypeName, \
				place.plc_id as _parentPlaceId, \
				parentplace."Name" as _parentPlaceName, \
				parentplace.typ_id as _parentPlaceTypeId, \
				parentplacetype."Name" as _parentPlaceTypeName, \
				task."DateStart" as _paramStartDate, \
				prop."ValueProp" as _placeCoord, \
				paramres."NameGroup" as _placeNameGroup \
		FROM "Tepl"."ParamResPlc_cnt" paramlist \
		LEFT JOIN "Tepl"."ParametrResourse" paramres on paramlist."ParamRes_id" = paramres."ParamRes_id" \
		LEFT JOIN "Tepl"."Places_cnt" place on paramlist.plc_id = place.plc_id \
		LEFT JOIN "Tepl"."PlaceTyp_cnt" placetype on place.typ_id = placetype.typ_id  \
		LEFT JOIN "Tepl"."Places_cnt" parentplace on place.place_id = parentplace.plc_id \
		LEFT JOIN "Tepl"."PlaceTyp_cnt" parentplacetype on parentplace.typ_id = parentplacetype.typ_id \
		LEFT JOIN (SELECT * FROM "Tepl"."Task_cnt" WHERE tsk_typ = 2 AND "Aktiv_tsk" =  True) task on paramlist.prp_id = task.prp_id \
		LEFT JOIN (SELECT * FROM "Tepl"."PropPlc_cnt" WHERE prop_id IN (72, 73, 74)) prop on place.plc_id = prop.plc_id \
		WHERE paramlist.prp_id = $param_id'
		args = {'param_id': self.param_id}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result:
			coords = 'https://static-maps.yandex.ru/1.x/?ll=_coords_&l=map&size=450,350&pt=_coords_,flag&z=12'
			if not result[12] == None:
				placeCoord = coords.replace('_coords_', result[12])
			else:
				placeCoord = 'https://tsc96.ru/upload/iblock/a5a/a5a129ed8c830e2dcafec7426d4c95d1.jpg'
			data = {
				'paramTypeId': result[1],
				'paramName': result[2],
				'placeId': result[3],
				'placeName': result[4],
				'placeTypeId': result[5],
				'placeTypeName': result[6],
				'parentPlaceId': result[7],
				'parentPlaceName': result[8],
				'parentPlaceTypeId': result[9],
				'parentPlaceTypeName': result[10],
				'paramStartDate': result[11].replace(tzinfo=None),
				'placeCoord': placeCoord,
				'placeNameGroup': result[13]
			}	
			self.metadata = data
		else:
			raise Exception('Ошибка загрузки метаданных')

	def defineParameterType(self):
		if self.metadata['paramTypeId'] in (1,):
			self.parameterType = 1 # 1 - delta (volume)
			return
		if self.metadata['paramTypeId'] in (269, 308): 
			self.parameterType = 2 # 2 - value (pressure)
			return
		else:
			raise Exception('Тип данных не учитывается')

	def definePlaceType(self):
		if self.metadata['placeTypeId'] in (20,):
			self.placeType = 1 # 1 = Куст (для баланса)
			self.place_id = self.metadata['placeId']

	def setNewestArchiveTime(self):
		query = ' SELECT MAX("DateValue") FROM "Tepl"."Arhiv_cnt" WHERE pr_id = $param_id AND typ_arh = 1 '
		args = {'param_id': self.param_id}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result:
			self.newestArchiveTime = result[0]
		else:
			raise Exception('Нет архивных данных')

	def defineConnectionStatus(self): #1
		if (datetime.now() - self.newestArchiveTime) < timedelta(hours = config.pollinterval * 2 + 1):
			self.connectionActive = True
			return
		self.connectionActive = False

	def initialize(self):
		try:
			if self.checkParameterExists():
				self.setParameterMetadata()
				self.defineParameterType()
				self.definePlaceType()
				self.setNewestArchiveTime()
				self.defineConnectionStatus()
		except Exception as e:
			self.edescription = e
			return False
		else:
			return True
		


