from functions_db import db
import functions_stuff as stuff
from datetime import datetime, timedelta, date, time
from config_functions import config

class parameterResource:
	def __init__(self, param_id):
		self.param_id = param_id
		self.metadata = None
		self.placeType = None
		self.parameterType = None
		self.initCompleted = False
		self.edescription = ''
		self.error = None
		self.connectionActive = False
		if self.initialize():
			self.initCompleted = True
		if not self.checkConnectionLost():
			self.connectionActive = True

	def checkParameterExists(self):
		query = ' SELECT prp_id FROM "Tepl"."ParamResPlc_cnt" WHERE prp_id = $param_id '
		args = {'param_id': self.param_id}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result and result[0] == self.param_id:
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
		if result and len(result) > 0:
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
			return
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

	def initialize(self):
		try:
			if self.checkParameterExists():
				self.setParameterMetadata()
				self.defineParameterType()
				self.definePlaceType()
		except Exception as e:
			self.edescription = e
			self.error = True
			return False
		else:
			return True
		
	def getNewestArchiveTime(self):
		if self.initCompleted:
			query = ' SELECT MAX("DateValue") FROM "Tepl"."Arhiv_cnt" WHERE pr_id = $param_id AND typ_arh = 1 '
			args = {'param_id': self.param_id}
			query = db.queryPrepare(query, args)
			result = db.fetchAll(query)
			if len(result) == 0:
				raise Exception('Нет архивных данных')
			else:
				return result[0]

	def checkConnectionLost(self): #1
		if self.initCompleted:
			newestArchiveTime = self.getNewestArchiveTime()
			if (datetime.now() - newestArchiveTime) > timedelta(hours = pollinterval * 2 + 1)
				return True
			return False
