import psycopg2, os, json, smtplib, sys
from datetime import datetime, timedelta, date, time
from jinja2 import Template
from configparser import ConfigParser
from email.mime.text import MIMEText
from email.header    import Header

# !временно инициализация параметров
averageweekdays = 7 # для ретроспективного вычисления среднего значения в определенный час 
pollhourinterval = 2 # интервал выхода на связь пульсаров (в часах) используется при вычислении среднего
pollhourdelta = 3 # лаг добавляемый при проверке (в часах)
dirsep = os.path.sep
folder = sys.path[0] + dirsep
configfile = folder + '..' + dirsep + 'config.ini'
templatefile = folder +'email.html'

def read_config(section):
    parser = ConfigParser()
    parser.read(configfile)
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, configfile))
 
    return db

# return: list список идентификаторов параметров, по которым идёт сбор данных
def getParamCheckList():
	query = ' \
	SELECT prp_id FROM "Tepl"."Task_cnt" WHERE tsk_typ = 2 AND "Aktiv_tsk" =  True'
	cursor = conn.cursor()
	cursor.execute(query)
	query = cursor.fetchall()
	params = []
	for item in query:
		for el in item:
			params.append(el)
	return params

# return: list ([0] - начало, [1] - конец)
def getWeekDateRange(lastDate):
	datalist = []
	datalist.append(lastDate - timedelta(days = averageweekdays))
	datalist.append(lastDate - timedelta(days = 1))
	return datalist

# return: list ([0] - начало, [1] - конец)
def getHourDateRange(lastDate):
	datalist = []
	datalist.append(lastDate - timedelta(hours = pollhourinterval))
	datalist.append(lastDate)
	return datalist

class resourceParameter:
	def __init__(self, param_id):
		self.param_id = param_id
		self.metadata = None
		self.placeType = None
		self.parameterType = None
		self.initCompleted = False
		a = self.loadStats()
		if a['success'] and a['result']:
			self.initCompleted = True
		else:
			self.error = a['error']
			self.description = a['description']

	def checkParameterExists(self):
		query = ' SELECT prp_id FROM "Tepl"."ParamResPlc_cnt" WHERE prp_id = %s '
		args = (self.param_id,)
		try:
			cursor.execute(query, args)
			query = cursor.fetchone()
		except Exception as e:
			print(e)
			return {'success': False, 'error': e, 'description':'Ошибка чтения базы данных'}
		else:
			if query:
				return {'success': True, 'result': True}
		return {'success': True, 'result': False}

	def getParameterMetadata(self):
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
		WHERE paramlist.prp_id = %s'
		args = (self.param_id,)
		try:
			cursor.execute(query, args)
			query = cursor.fetchone()
		except Exception as e:
			print(e)
			return {'success': False, 'error': e, 'description':'Ошибка чтения базы данных'}
		else:
			if query:
				coords = 'https://static-maps.yandex.ru/1.x/?ll=_coords_&l=map&size=450,350&pt=_coords_,flag&z=12'
				if not query[12] == None:
					placeCoord = coords.replace('_coords_', query[12])
				else:
					placeCoord = 'https://tsc96.ru/upload/iblock/a5a/a5a129ed8c830e2dcafec7426d4c95d1.jpg'
				data = {
					'paramTypeId': query[1],
					'paramName': query[2],
					'placeId': query[3],
					'placeName': query[4],
					'placeTypeId': query[5],
					'placeTypeName': query[6],
					'parentPlaceId': query[7],
					'parentPlaceName': query[8],
					'parentPlaceTypeId': query[9],
					'parentPlaceTypeName': query[10],
					'paramStartDate': query[11].replace(tzinfo=None),
					'placeCoord': placeCoord,
					'placeNameGroup': query[13]
				}
				return {'success': True, 'result': data}
			return {'success': True, 'result': None}
	
	def defineParameterType(self):
		if self.metadata['paramTypeId']:
			if self.metadata['paramTypeId'] in (1,):
				return {'success': True, 'result': 1} # 1 - delta (volume)
			if self.metadata['paramTypeId'] in (269, 308): 
				return {'success': True, 'result': 2} # 2 - value (pressure)
			return {'success': False, 'error': True, 'description': 'Тип данных не учитывается'} 
		return {'success': False, 'error': True, 'description': 'Ошибка чтения метаданных. Тип параметра не определён' }

	def definePlaceType(self):
		if self.metadata['placeTypeId']:
			if self.metadata['placeTypeId'] in (20,):
				return {'success': True, 'result': 1} # 1 = Куст (для баланса)
			return {'success': True, 'result': None}
		return {'success': False, 'error': True, 'description': 'Ошибка чтения метаданных. Тип объекта (места) не определён'}

	def loadStats(self):
		ex = self.checkParameterExists()
		if ex['success'] and ex['result']:
			pm = self.getParameterMetadata()
			if pm['success'] and pm['result']:
				self.metadata = pm['result']
				pt = self.defineParameterType()
				if pt['success'] and pt['result']:
					self.parameterType = pt['result']
					plt = self.definePlaceType()
					if plt['success'] and plt['result']:
						self.placeType = plt['result']
					return {'success': True, 'result': True}
				else:
					return {'success': False, 'error': pt['error'], 'description': pt['description']}
			else:
				return {'success': False, 'error': pm['error'], 'description': pm['description']}					
		else:
			return {'success': False, 'error': ex['error'], 'description': ex['description']}				

class parameterIncidents(resourceParameter):
	def __init__(self, id):
		resourceParameter.__init__(self, id)
		self.last = None
		self.dataLoaded = False
		a = self.getLastArchive()
		if a['success'] and a['result']:
			self.last = a['result']
			self.dataLoaded = True
		else:
			self.error = a['error']
			self.description = a['description']	

	def getLastArchiveTime(self):
		if self.initCompleted:
			query = ' SELECT MAX("DateValue") FROM "Tepl"."Arhiv_cnt" WHERE pr_id = %s AND typ_arh = 1'
			args = (self.param_id,)
			try:
				cursor.execute(query, args)
				query = cursor.fetchone()
			except Exception as e:
				print(e)
				return {'success': False, 'error': e, 'description': 'Ошибка чтения базы данных'}
			else:	
				if query[0]:
					return {'success': True, 'result': query[0]}
				return {'success': False, 'error': True, 'description': 'Последняя дата не определена'}
		else:
			return {'success': False, 'error': self.error, 'description': self.dataLoaded}

	def getLastArchive(self):
		if self.initCompleted:
			query = ' \
			SELECT "DataValue", "Delta" FROM "Tepl"."Arhiv_cnt" \
			WHERE pr_id = %s AND typ_arh = 1 \
			AND "DateValue" = %s '
			a = self.getLastArchiveTime()
			if a['success']:
				lastArchiveTime = a['result']
				args = (self.param_id, lastArchiveTime)
				try:
					cursor.execute(query, args)
					query = cursor.fetchone()
				except Exception as e:
					print(e)
					return {'success': False, 'error': e, 'description': 'Ошибка чтения базы данных'}
				else:	
					if query[0]:
						if self.parameterType == 1:
							#self.lastArchiveData = round(query[1],2) 
							return {'success': True, 'result': {'time': lastArchiveTime, 'value': round(query[1],2)}}
						if self.parameterType == 2: 
							#self.lastArchiveData = round(query[0],2)
							return {'success': True, 'result': {'time': lastArchiveTime, 'value': round(query[0],2)}}
						return {'success': False, 'error': True, 'description': 'Этот тип параметра не учитывается'}
					return {'success': False, 'error': True, 'description': 'Последнее значение не определено'}
			else:
				return {'success': False, 'error': a['error'], 'description': a['description'] }
		else:
			return {'success': False, 'error': self.error, 'description': self.dataLoaded}
	
	def checkConnectionLost(self): #1
		if self.dataLoaded:
			if (datetime.now() - timedelta(hours = pollhourinterval + pollhourdelta)) > self.last['time']:
				return {'success': True, 'result': True}
			else:
				return {'success': True, 'result': False}
		return {'success': False, 'error': True, 'description': 'Последние данные не загружены'}
		
	def getAverageValue(self, timerange):
		if timerange[1] - timerange[0] > timedelta(hours = 24):
			rangetype = '1 day'
		else:
			rangetype = '1 hour'
		for item in timerange:
			item = str(item)
		query = '\
		DROP TABLE IF EXISTS date_range_tmp; \
		CREATE TEMPORARY TABLE date_range_tmp("DateValue" timestamp without time zone); \
		INSERT INTO date_range_tmp SELECT "Tepl"."GetDateRange"(%s, %s, %s);\
		SELECT * FROM date_range_tmp;\
		SELECT SUM(CASE WHEN %s = 1 THEN "Delta" ELSE "DataValue" END)/(SELECT COUNT(*) FROM date_range_tmp) FROM "Tepl"."Arhiv_cnt"\
		WHERE pr_id = %s AND typ_arh = 1 AND "DateValue" IN (SELECT * FROM date_range_tmp);'
		args = (timerange[0], timerange[1], rangetype, self.parameterType, self.param_id)
		try:
			cursor.execute(query, args)
			query = cursor.fetchone()
		except Exception as e:
			print(e)
			return {'success': False, 'error': e, 'description': 'Ошибка чтения базы данных'}
		else:	
			if query[0]:
				return {'success': True, 'result': query[0]}
			return {'success': False, 'error': True, 'description': 'Среднее значение не определено'}

	def checkConsumptionUp(self):
		if self.dataLoaded:
			if not (self.metadata['paramStartDate'] + timedelta(days = averageweekdays)) <= self.last['time']:
				return {'success': False, 'error': True, 'description': "С начала сбора данных прошло слишком мало времени. Определить среднее значение невозможно" }
			else:
				range = getWeekDateRange(self.last['time'])
				a = self.getAverageValue(range)
				if a['success']:
					averageValue = a['result']
					if averageValue >= 0 :
						if self.last['value'] > (averageValue * 2):
							return {'success': True, 'result': True}
						else:
							return {'success': True, 'result': False}
					else:
						return {'success': False, 'error': True, 'description': "Ошибка. Среднее значение имеет отрицательную величину" }
				else:
					return {'success': False, 'error': a['error'], 'description': a['description'] }


#==================================
db_config = read_config('database')
email_config = read_config('email')

print(datetime.now())
print("Connecting to database...")
try:
	conn = psycopg2.connect(**db_config)
	cursor = conn.cursor()
except psycopg2.OperationalError as e:
	print(e)
	print('Connection failed.')
	exit(0)
else:
	print('Connected!')



a = parameterIncidents(9)
b = a.getLastArchiveTime()
if b['success']:
	c = b['result']
	print(c)
	d = a.getLastArchive()
	if d['success']:
		e = d['result']
		print(e)
pass