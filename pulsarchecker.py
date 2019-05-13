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
autoClosableIncidentTypes = [1,5]
dirsep = os.path.sep
folder = sys.path[0] + dirsep
configfile = folder + '..' + dirsep + 'pulsarchecker.config'

balanceQueryFile = folder + dirsep + 'balance.sql'
incidentsNoticeTemplate = folder +'email.html'
balanceNoticeTemplate = folder +'balance.html'
dailyReportNoticeTemplate = folder +'dailyreport.html'
#+
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
#+
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
#+
# return: list ([0] - начало, [1] - конец)
def getWeekDateRange(lastDate):
	datalist = []
	datalist.append(lastDate - timedelta(days = averageweekdays))
	datalist.append(lastDate - timedelta(days = 1))
	return datalist
#+
# return: list ([0] - начало, [1] - конец)
def getHourDateRange(lastDate):
	datalist = []
	datalist.append(lastDate - timedelta(hours = pollhourinterval))
	datalist.append(lastDate)
	return datalist
#+
def getDatesByHour(date_s, date_e):
	query = ' SELECT "Tepl"."GetDateRange"($date_s, $date_e, \'1 hour\' ) '
	args = {'date_s': date_s, 'date_e': date_e}
	query = prepareQuery(query, args)
	query = queryFetchAll(query)
	if query['success']:
		return query['result']
#+
def getDatesByDays(date_s, date_e):
	query = ' SELECT "Tepl"."GetDateRange"($date_s, $date_e, \'1 day\' ) '
	args = {'date_s': date_s, 'date_e': date_e}
	query = prepareQuery(query, args)
	query = queryFetchAll(query)
	if query['success']:
		return query['result']

#+
# подставить значение переменных в sql запрос
def prepareQuery(query, args):
	arguments = {}
	for arg in args:
		a = type(args[arg]).__name__
		if a == 'date' or a == 'datetime' or a == 'str':
			arguments[arg] = '\'' + str(args[arg]) + '\''
		else: 
			arguments[arg] = str(args[arg])
		find = "$" + str(arg)
		replacewith = arguments[arg]
		query = query.replace(find, replacewith)
	return query
#-
# выполнить query fetchone
def queryFetchOne(query):
	try:
		cursor.execute(query)
		query = cursor.fetchone()
	except Exception as e:
		print(e)
		message = 'При чтении базы данных случилась ошибка: \n' + str(e)
		header = 'Ошибка мониторинга'
		sendEmail(header, message)
		return {'success': False, 'error': e, 'edescription': 'Ошибка чтения базы данных' }
	else:
		if query:	
			if len(query) == 1:
				return  {'success': True,  'result': query[0]}
			else:
				return  {'success': True,  'result': query}
		return  {'success': True,  'result': query}
#+
def queryFetchAll(query):
	try:
		cursor.execute(query)
		query = cursor.fetchall()
	except Exception as e:
		print(e)
		message = 'При чтении базы данных случилась ошибка: \n' + str(e)
		header = 'Ошибка мониторинга'
		sendEmail(header, message)
		return {'success': False, 'error': e, 'edescription': 'Ошибка чтения базы данных' }
	else:
		return  {'success': True,  'result': query}
#+
# выполнить инсерт или апдейт
def queryUpdate(query):
		try:
			cursor.execute(query)
		except Exception as e:
			print(e)
			conn.rollback()
			message = 'При записи изменений в базу данных случилась ошибка: \n' + str(e)
			header = 'Ошибка мониторинга'
			sendEmail(header, message)
			return {'success': False, 'error': e, 'description':'Ошибка записи изменений в базу данных'}
		else:
			conn.commit()
		return {'success': True, 'result': None}
#+
def updateIncidentRegister(param_id, lastchecked_time, regtype):
	query = ' SELECT count(*) FROM "Tepl"."Alerts_register" where param_id = $param_id and regtype= $regtype'
	queryResult = queryFetchOne(prepareQuery(query, {'param_id': param_id, 'regtype': regtype}))
	if queryResult['success']:
		args = {'param_id': param_id, 'lastchecked_time': lastchecked_time, 'regtype': regtype}
		if queryResult['result']:
			query = ' UPDATE "Tepl"."Alerts_register" SET lastchecked_time = $lastchecked_time WHERE param_id = $param_id and regtype= $regtype; '
		else:
			query = ' INSERT INTO "Tepl"."Alerts_register"(param_id, lastchecked_time, regtype)  VALUES ($param_id, $lastchecked_time, $regtype);  '
		queryUpdate(prepareQuery(query, args))
#+
def getIncidentRegisterDate(param_id, regtype):
	query = ' SELECT lastchecked_time FROM "Tepl"."Alerts_register" where param_id = $param_id and regtype = $regtype '
	queryResult = queryFetchOne(prepareQuery(query, {'param_id': param_id, 'regtype': regtype}))
	if queryResult['success']:
		return{'success': True, 'result': queryResult['result']}
	return queryResult

#+
class resourceParameter:
	def __init__(self, param_id):
		self.param_id = param_id
		self.metadata = None
		self.placeType = None
		self.parameterType = None
		self.initCompleted = False
		self.edescription = ''
		self.error = None
		a = self.loadStats()
		if a['success'] and a['result']:
			self.initCompleted = True
		else:
			self.error = a['error']
			self.edescription = a['description']

	def checkParameterExists(self):
		query = ' SELECT prp_id FROM "Tepl"."ParamResPlc_cnt" WHERE prp_id = $param_id '
		query = queryFetchOne(prepareQuery(query, {'param_id': self.param_id}))
		if query['success']:
			if query['result']:
				return {'success': True, 'result': True}
			else:
				return {'success': False, 'error': True, 'description':'Такого параметра не существует'}
		return query

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
		WHERE paramlist.prp_id = $param_id'
		query = queryFetchOne(prepareQuery(query, {'param_id': self.param_id}))
		if query['success']:
			if query['result']:
				coords = 'https://static-maps.yandex.ru/1.x/?ll=_coords_&l=map&size=450,350&pt=_coords_,flag&z=12'
				if not query['result'][12] == None:
					placeCoord = coords.replace('_coords_', query['result'][12])
				else:
					placeCoord = 'https://tsc96.ru/upload/iblock/a5a/a5a129ed8c830e2dcafec7426d4c95d1.jpg'
				data = {
					'paramTypeId': query['result'][1],
					'paramName': query['result'][2],
					'placeId': query['result'][3],
					'placeName': query['result'][4],
					'placeTypeId': query['result'][5],
					'placeTypeName': query['result'][6],
					'parentPlaceId': query['result'][7],
					'parentPlaceName': query['result'][8],
					'parentPlaceTypeId': query['result'][9],
					'parentPlaceTypeName': query['result'][10],
					'paramStartDate': query['result'][11].replace(tzinfo=None),
					'placeCoord': placeCoord,
					'placeNameGroup': query['result'][13]
				}				
				return {'success': True, 'result': data}
			return {'success': False, 'error': True, 'description':'По данному параметру нет метаданных'}
	
	def defineParameterType(self):
		if self.metadata['paramTypeId']:
			if self.metadata['paramTypeId'] in (1,):
				return {'success': True, 'result': 1} # 1 - delta (volume)
			if self.metadata['paramTypeId'] in (269, 308): 
				return {'success': True, 'result': 2} # 2 - value (pressure)
			return {'success': False, 'error': True, 'description': 'Тип данных не учитывается'} 
		return {'success': False, 'error': True, 'description': 'Ошибка метаданных. Тип параметра не определён' }

	def definePlaceType(self):
		if self.metadata['placeTypeId']:
			if self.metadata['placeTypeId'] in (20,):
				return {'success': True, 'result': 1} # 1 = Куст (для баланса)
			return {'success': True, 'result': None}
		return {'success': False, 'error': True, 'description': 'Ошибка метаданных. Тип объекта (места) не определён'}

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
		self.last = {}
		self.dataLoaded = False
		if self.initCompleted:
			a = self.getNewestArchiveTime()
			if a is True:
				a = self.getLastCheckedTime()
				if a['success'] and a['result']:
					self.last['lastCheckedTime'] = a['result']
					a = self.getCurrenArchiveValue()
					if a['success']:
						self.last['lastArchiveValue'] = a['result']
						self.dataLoaded = True
						return
			self.error = a['error']
			self.edescription = a['description']	
#+
	def getNewestArchiveTime(self):
		if self.initCompleted:
			query = ' SELECT MAX("DateValue") FROM "Tepl"."Arhiv_cnt" WHERE pr_id = $param_id AND typ_arh = 1 '
			query = queryFetchOne(prepareQuery(query, {'param_id': self.param_id}))
			if query['success']:
				if query['result']:
					self.last['newestArchiveTime'] = query['result']
					return True
				return {'success': False, 'error': True, 'description': 'Последняя дата не определена'}
			return query
		return {'success': False, 'error': self.error, 'description': self.edescription}
#-
	def getLastCheckedTime(self):
		if self.initCompleted:
			a = getIncidentRegisterDate(self.param_id, 'incident')
			if a['success']:
				if a['result']:
					return {'success': True, 'result': a['result']}
				else: 
					a = self.getNewestArchiveTime() 
					if a is True:
						updateIncidentRegister(self.param_id, self.last['newestArchiveTime'], 'incident')
						return {'success': True, 'result': a['result'] }
					return a
			return a
		return {'success': False, 'error': self.error, 'description': self.edescription}
#+
	def getCurrenArchiveValue(self):
		if self.initCompleted:
			a = self.getLastCheckedTime()
			if a['success'] and a['result']:
				lastCheckedTime = a['result']
				query = ' SELECT "DataValue", "Delta" FROM "Tepl"."Arhiv_cnt" WHERE pr_id = $param_id AND typ_arh = 1 AND "DateValue" = $lastCheckedTime '	
				query = prepareQuery(query, {'param_id': self.param_id, 'lastCheckedTime': lastCheckedTime})			
				query = queryFetchOne(query)
				if query['success']:
					if query['result']:
						if self.parameterType == 1:
							self.last['lastArchiveValue'] = round(query['result'][1],2)
							return {'success': True, 'result': round(query['result'][1],2)}
						if self.parameterType == 2: 
							self.last['lastArchiveValue'] = round(query['result'][0],2)
							return {'success': True, 'result': round(query['result'][0],2)}
						return {'success': False, 'error': True, 'description': 'Этот тип параметра не учитывается'}
					return {'success': False, 'error': True, 'description': 'Последнее значение не определено'}
				return query
			return {'success': False, 'error': a['error'], 'description': a['description'] }
		return {'success': False, 'error': self.error, 'description': self.edescription}
#+
	def checkConnectionLost(self): #1
		if self.dataLoaded:
			if (datetime.now() - self.last['newestArchiveTime']) > timedelta(hours = pollhourinterval + pollhourdelta):
				return {'success': True, 'result': True}
			else:
				return {'success': True, 'result': False}
		else:
			return {'success': False, 'error': self.error, 'description': self.edescription}
#+		
	def getAverageValue(self, timerange):
		if self.dataLoaded:
			if timerange[1] - timerange[0] > timedelta(hours = 24):
				rangetype = "1 day"
			else:
				rangetype = "1 hour"
			query = '\
			DROP TABLE IF EXISTS date_range_tmp; \
			CREATE TEMPORARY TABLE date_range_tmp("DateValue" timestamp without time zone); \
			INSERT INTO date_range_tmp SELECT "Tepl"."GetDateRange"($date_s, $date_e, $rangetype);\
			SELECT * FROM date_range_tmp;\
			SELECT SUM(CASE WHEN $parameterType = 1 THEN "Delta" ELSE "DataValue" END)/(SELECT COUNT(*) FROM date_range_tmp) FROM "Tepl"."Arhiv_cnt"\
			WHERE pr_id = $param_id AND typ_arh = 1 AND "DateValue" IN (SELECT * FROM date_range_tmp);'
			args = {'date_s': timerange[0], 'date_e': timerange[1], 'rangetype': rangetype, 'parameterType': self.parameterType, 'param_id': self.param_id}
			query = queryFetchOne(prepareQuery(query, args))
			if query['success']:
				if query['result'] == 0 or query['result']:
					return {'success': True, 'result': query['result']}
				return {'success': False, 'error': True, 'description': 'Среднее значение не определено'}
			return query
		return {'success': False, 'error': self.error, 'description': self.edescription}
#+
	def checkConsumptionUp(self):
		if self.dataLoaded:
			if not self.last['lastCheckedTime'] >= (self.metadata['paramStartDate'] + timedelta(days = averageweekdays)):
				return {'success': False, 'error': True, 'description': "С начала сбора данных прошло слишком мало времени. Определить среднее значение невозможно" }
			else:
				range = getWeekDateRange(self.last['lastChecked'])
				a = self.getAverageValue(range)
				if a['success']:
					averageValue = a['result']
					if averageValue >= 0 :
						if self.last['lastArchiveValue'] > (averageValue * 2):
							return {'success': True, 'result': True}
						else:
							return {'success': True, 'result': False}
					else:
						return {'success': False, 'error': True, 'description': "Ошибка. Среднее значение имеет отрицательную величину" }
				else:
					return {'success': False, 'error': a['error'], 'description': a['description'] }
		else:
			return {'success': False, 'error': self.error, 'description': self.edescription}
#+
	def checkConsumptionStale(self):
		if self.dataLoaded:
			if not self.last['lastCheckedTime'] >= (self.metadata['paramStartDate'] + timedelta(hours = pollhourinterval)):
				return {'success': False, 'error': True, 'description': "С начала сбора данных прошло слишком мало времени. Определить среднее значение невозможно" }
			else:
				range = getHourDateRange(self.last['lastChecked'])
				a = self.getAverageValue(range)
				if a['success']:
					averageValue = a['result']
					if averageValue == 0:
						return {'success': True, 'result': True}
					else:
						return {'success': True, 'result': False}
				else:
					return {'success': False, 'error': a['error'], 'description': a['description'] }
		else:
			return {'success': False, 'error': self.error, 'description': self.edescription}
#+
	def checkValueDown(self):
		if self.dataLoaded:
			if not  self.last['lastCheckedTime'] >= (self.metadata['paramStartDate'] + timedelta(hours = pollhourinterval)):
				return {'success': False, 'error': True, 'description': "С начала сбора данных прошло слишком мало времени. Определить среднее значение невозможно" }
			else:
				range = getHourDateRange(self.last['lastChecked'])
				a = self.getAverageValue(range)
				if a['success']:
					averageValue = a['result']
					if (averageValue > self.last['lastArchiveValue'] * 2):
						return {'success': True, 'result': True}
					else:
						return {'success': True, 'result': False}
				else:
					return {'success': False, 'error': a['error'], 'description': a['description'] }					
		else:
			return {'success': False, 'error': self.error, 'description': self.edescription}
#+
	def getCurrentIncident(self):
		if not self.initCompleted:
			return {'success': True, 'result': {'incidentType': 5, 'description': 'Параметр не инициализирован.', 'self': self}}
		else: 
			if not self.dataLoaded:
				return {'success': True, 'result': {'incidentType': 5, 'description': 'Параметр не инициализирован.', 'self': self}}
			else:
				inc = self.checkConnectionLost()
				if inc['success']:
					if inc['result']:
						return {'success': True, 'result': {'incidentType': 1, 'description': 'Прибор не вышел на связь в установленное время.', 'self': self}}
					if self.parameterType == 1: # 1 - delta (volume)
						inc = self.checkConsumptionUp()	
						if inc['success']:
							if inc['result']:
								return {'success': True, 'result': {'incidentType': 2, 'description': 'Зафиксировано повышение расхода контроллируемого параметра.', 'self': self}}
							inc = self.checkConsumptionStale()
							if inc['success']:
								if inc['result']:
									return {'success': True, 'result': {'incidentType': 3, 'description': 'Зафиксировано отсутствие расхода.', 'self': self}}
							else:
								{'success': True, 'result': {'incidentType': 3, 'description': inc['description'], 'self': self}}
						else:
							return {'success': True, 'result': {'incidentType': 2, 'description': inc['description'], 'self': self}}
					if self.parameterType == 2: # 2 - value (pressure)
						inc = self.checkConsumptionUp()
						if inc['success']:
							if inc['result']:
								return {'success': True, 'result': {'incidentType': 4, 'description': 'Зафиксировано падение значения параметра.', 'self': self}}
						else:
							return {'success': True, 'result': {'incidentType': 4, 'description': inc['description'], 'self': self}}
				else: 
					return {'success': True, 'result': {'incidentType': 1, 'description': inc['description'], 'self': self}}

		return {'success': True, 'result': None}

#+
class parameterBalance(resourceParameter):
	def __init__(self, id, date):
		resourceParameter.__init__(self, id)
		self.date_s = date
		self.date_e = date
		self.balance = []
		self.balanceMessage = ''
		self.place = self.metadata['placeId']
		filedata = open(balanceQueryFile,'r')
		query = filedata.read()
		arguments = {'date_s': self.date_s, 'date_e': self.date_e, 'place': self.place, 'type_a': 2}
		self.query = prepareQuery(query, arguments)
#+
	def checkBalanceAvailability(self, date):
		query = ' \
		SELECT places."Name" \
		FROM "Tepl"."ParamResPlc_cnt" prp \
		INNER JOIN "Tepl"."Places_cnt" places on prp.plc_id = places.plc_id \
		LEFT JOIN "Tepl"."Places_cnt" parentplaces on places.plc_id = parentplaces.place_id \
		WHERE places."Name" not in ( \
		SELECT places."Name" \
		FROM "Tepl"."ParamResPlc_cnt" prp \
		INNER JOIN "Tepl"."Places_cnt" places on prp.plc_id = places.plc_id \
		LEFT JOIN "Tepl"."Places_cnt" parentplaces on places.plc_id = parentplaces.place_id \
		LEFT JOIN "Tepl"."Arhiv_cnt" arhiv on prp.prp_id = arhiv.pr_id \
		WHERE places.place_id = $place_id and prp."ParamRes_id" = 1 and arhiv."DateValue" = $date and arhiv.typ_arh = 2 \
		) and places.place_id = $place_id and prp."ParamRes_id" = 1;	'
		args = {'place_id': self.place, 'date': date}
		query = prepareQuery(query, args)
		query = queryFetchAll(query)
		if query['success']:
			if len(query['result']) > 0:
				adresses = []
				for item in query['result']:
					adresses.append(item[0])
				return {'success': False, 'result': adresses}
			return {'success': True}
		return query
#+		
	def getBalanceStats(self):
		if self.query:
			query = queryFetchOne(self.query)
			if query['success']:
				return {'success': True, 'result': query['result']}
			return query
#+
	def getBalanceMessage(self):
		a = self.getBalanceStats()
		if a['success'] and a['result']:
			balance = a['result']
			for item in balance:
				if type(item).__name__ == 'float':
					item = round(item, 1)
				self.balance.append(item)
			place = self.metadata['placeTypeName'] + ' ' + self.metadata['placeName']
			self.balanceMessage = fillEmailTemplate(balanceNoticeTemplate, {place: self.balance} )
		return self.balanceMessage	

class dailyReport():
	def __init__(self, date):
		self.date_s = date
		self.date_e = date + timedelta(days = 1)
#+
	def getIncidentsStats(self):
		stats = {}
		args = {'date_s': self.date_s, 'date_e': self.date_e}
		query = queryFetchOne(' SELECT COUNT(id) FROM "Tepl"."Alert_cnt" WHERE status = \'active\' ')
		if query['success']:
			stats['Активных инцидентов на данный момент'] = query['result']	

		query = ' SELECT COUNT(id) FROM "Tepl"."Alert_cnt" WHERE created_at > $date_s and created_at < $date_e '
		query = prepareQuery(query, args)
		query = queryFetchOne(query)
		if query['success']:
			stats['Создано новых инцидентов за прошедший день'] = query['result']	

		query = ' SELECT COUNT(id) FROM "Tepl"."Alert_cnt" WHERE status = \'autoclosed\' and updated_at > $date_s and updated_at < $date_e '
		query = queryFetchOne((prepareQuery(query, args)))
		if query['success']:
			stats['Инцидентов закрытых автоматически']  = query['result']

		query = ' SELECT COUNT(id) FROM "Tepl"."Alert_cnt" WHERE status = \'closed\' and updated_at > $date_s and updated_at < $date_e '
		query = queryFetchOne((prepareQuery(query, args)))
		if query['success']:
			stats['Инцидентов закрытых вручную'] = query['result']

		return stats
#+
	def getReportMessage(self):
		subst = self.getIncidentsStats()
		dailyReportMessage = fillEmailTemplate(dailyReportNoticeTemplate, subst)
		return dailyReportMessage
#+
class incidentHandler:
	def saveIncident(self, incident):
		if len(incident) > 0:
			query = 'INSERT INTO "Tepl"."Alert_cnt"("time", param_id, type, param_name, place_id, "PARENT", "CHILD", description, staticmap, namegroup, lastarchivedata, lastchecked_time) \
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); '
			if not incident['self'].last.get('lastChecked'):
				d = date(2000, 1, 1)
				t = time(00, 00)
				incident['self'].last['lastChecked'] = datetime.combine(d, t)
			args = (
			incident['self'].last['lastChecked'],
			incident['self'].param_id,
			incident['incidentType'],
			incident['self'].metadata['paramName'],
			incident['self'].metadata['placeId'],
			incident['self'].metadata['parentPlaceTypeName'] + ' ' + incident['self'].metadata['parentPlaceName'],
			incident['self'].metadata['placeTypeName'] + ' ' + incident['self'].metadata['placeName'],			
			incident['description'] + ' ' + incident['self'].edescription,
			incident['self'].metadata['placeCoord'],
			incident['self'].metadata['placeNameGroup'],
			incident['self'].last.get('lastArchiveValue'),
			incident['self'].last['lastChecked']
			)
			try:
				cursor.execute(query, args)
			except Exception as e:
				print(e)
				conn.rollback()
				return {'success': False, 'error': e, 'description':'Ошибка записи в базу данных'}
			else:
				conn.commit()
				return {'success': True, 'result': True}

	def closeIncident(self, incident_id, close_type):
		if close_type == 1:
			status = 'autoclosed'
		else:
			status = 'closed'
		query = 'UPDATE "Tepl"."Alert_cnt" SET status = %s WHERE id = %s '
		args = (status, incident_id)
		try:
			cursor.execute(query, args)
		except Exception as e:
			print(e)
			conn.rollback()
			return {'success': False, 'error': e, 'description':'Ошибка записи в базу данных'}
		else:
			conn.commit()
			return {'success': True, 'result': cursor.lastrowid}

	def getExistingIncident(self, param_id, incident_type):
		query = 'SELECT MAX(id) FROM "Tepl"."Alert_cnt" WHERE status = \'active\' and param_id = %s and type = %s'
		args = (param_id, incident_type)
		try:
			cursor.execute(query, args)
			query = cursor.fetchone()
		except Exception as e:
			print(e)
			return {'success': False, 'error': e, 'description':'Ошибка чтения базы данных'}
		else:
			if query:
				return {'success': True, 'result': query[0]}
			return {'success': False, 'result': None}		

	def getExistingIncidentTime(self, incident_id):
		query = 'SELECT time FROM "Tepl"."Alert_cnt" WHERE id = $incident_id'
		query = queryFetchOne(prepareQuery(query, {'incident_id': incident_id}))
		if query:
			return {'success': True, 'result': query['result']}
		return {'success': False, 'result': None}		

	def getExistingIncidentLastCheckedTime(self, incident_id):
		query = 'SELECT lastchecked_time FROM "Tepl"."Alert_cnt" WHERE id = $incident_id'
		query = queryFetchOne(prepareQuery(query, {'incident_id': incident_id}))
		if query:
			return {'success': True, 'result': query['result']}
		return {'success': False, 'result': None}		

	def updateExistingIncidentLastCheckedTime(self, incident_id, lastchecked_time):
		query = 'UPDATE "Tepl"."Alert_cnt" SET lastchecked_time = $lastchecked_time WHERE id = $incident_id '
		args = {'incident_id': incident_id, 'lastchecked_time': lastchecked_time}
		query = prepareQuery(query, args)
		query = queryUpdate(query)
#+
def structureIncidents(incidents):
	emailsubst = {}
	for incident in incidents:
		_parent = incident['self'].metadata['parentPlaceTypeName'] + ' ' + incident['self'].metadata['parentPlaceName']
		if _parent not in emailsubst:
			emailsubst[_parent] = {}
		_child = incident['self'].metadata['placeTypeName'] + ' ' + incident['self'].metadata['placeName']
		if _child not in emailsubst[_parent]:
			emailsubst[_parent][_child] = []
		params = (incident['self'].metadata['paramName'], incident['description'] + ' ' + incident['self'].edescription)
		emailsubst[_parent][_child].append(params)
	return emailsubst
#+
def fillEmailTemplate(templatefile, subst):
	if len(subst) > 0:
		html = open(templatefile).read()
		template = Template(html)
		message = template.render(subst=subst)
		return message
#+
def sendEmail(header, message):
		recipients_emails = email_config['recipients_emails'].split(',')
		msg = MIMEText(message, 'html', 'utf-8')
		msg['Subject'] = Header(header, 'utf-8')
		msg['From'] = "Система мониторинга <monitoring@rsks.su>"
		msg['To'] = ", ".join(recipients_emails)
		server = smtplib.SMTP(email_config['host'])
		server.sendmail(msg['From'], recipients_emails, msg.as_string())
		server.quit()

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

savedIncidentCounter = 0
autoclosedIncidentCounter = 0
savedincidents = []

if len(sys.argv) == 1:
	parametersList = getParamCheckList()
	for param_id in parametersList:
		iHandler = incidentHandler()
		pIncident = parameterIncidents(param_id)
		activeLostIncident = False
		activeOtherIncident = False
		activeOtherIncidentTime = False
		activeLostIncidentTime = False

		# Проверка наличия активного инцидента connection lost
		a = iHandler.getExistingIncident(param_id, 1)
		if a['success']:
			if a['result']:
				activeLostIncident = a['result']
				a= iHandler.getExistingIncidentTime(activeLostIncident)
				if a['success']:
					if a['result']:
						activeLostIncidentTime = a['result']

		a = iHandler.getExistingIncident(param_id, 5)
		if a['success']:
			if a['result']:
				activeOtherIncident = a['result']
				a= iHandler.getExistingIncidentTime(activeOtherIncident)
				if a['success']:
					if a['result']:
						activeOtherIncidentTime = a['result']

		# Проверка связи
		a = pIncident.checkConnectionLost()
		if a['success']:
			connectionLost = a['result']
		else:
			connectionLost = True

		if connectionLost:  # если связь потеряна
			if activeLostIncident or activeOtherIncident: # и активный инцидент connection lost (или тип:другой) существует , то переходим к следующему параметру
				continue
			else: # если активного инцидента нет, то его нужно создать
				a = pIncident.getCurrentIncident()
				if a['success']:
					incident = a['result']
					iHandler.saveIncident(incident)
					savedIncidentCounter += 1
					savedincidents.append(incident)
					# и обновить время последней проверки в регистре
					updateIncidentRegister(pIncident.param_id, pIncident.last['newestArchiveTime'], 'incident')
					continue
		else: # если связь в порядке
			if activeLostIncident: # если есть активный инцидент connection lost, то его надо закрыть
				iHandler.closeIncident(activeLostIncident, 1)
				autoclosedIncidentCounter += 1
				# и обновить время последней проверки в регистре
				updateIncidentRegister(pIncident.param_id, pIncident.last['newestArchiveTime'], 'incident')

			# перебираем все инциденты за время прошедшее с момента последней проверки
			datesTuple= getDatesByHour(pIncident.last['lastCheckedTime'], pIncident.last['newestArchiveTime'])
			if len(datesTuple) == 1:
				print('Параметр ' + str(pIncident.param_id) + ' уже проверен ранее')
			else:
				print('Параметр: ' + str(pIncident.param_id) + '. количество пропущенных точек проверки: ' + str(len(datesTuple)))
				for dateTuple in datesTuple:
					date = dateTuple[0]
					print(date)
					pIncident.last['lastChecked'] = date
					a = pIncident.getCurrentIncident()
					if a['success'] and a['result']: # если в этом промежутке найден инцидент
						incident = a['result']
						incidentType = incident['incidentType']
						# проверяем наличие инцидента такого же типа
						a = iHandler.getExistingIncident(pIncident.param_id, incidentType)
						if a['success'] and a['result']:
							activeTypedIncident = a['result']
							# если находим, то проверяем когда он был последний раз проверен
							a = iHandler.getExistingIncidentLastCheckedTime(activeTypedIncident)
							if a['success']:
								if a['result']:
									activeTypedIncidentTime = a['result']
									# вычисляем как давно он был проверен последний раз:
									incidentAge = date - activeTypedIncidentTime
									print('incident age: ' + str(incidentAge))
									if incidentAge > timedelta(hours = 1):	 # если последняя проверка инцидента была больше часа назад
										iHandler.saveIncident(incident)		 # то это уже новый инцидент (старый закрывается вручную)
										savedIncidentCounter += 1
										savedincidents.append(incident)
										continue 							 # т.е. если инцидент был непрерывен, то новых инцидентов создаваться не будет
									else: 
										iHandler.updateExistingIncidentLastCheckedTime(activeTypedIncident, date)
						# если такого же активного инцидента не находим, то его нужно создать
						else:
							iHandler.saveIncident(incident)
							savedIncidentCounter += 1
							savedincidents.append(incident)
							# и обновить время последней проверки в регистре
							updateIncidentRegister(pIncident.param_id, date, 'incident')			

				updateIncidentRegister(pIncident.param_id, date, 'incident')
			
# лирическое отступление: 
# pIncident.last['lastCheckedTime'] это время записаное в регистр
# pIncident.last['lastChecked'] это время последней проверки в цикле

	print('Новых инцидентов: ' + str(savedIncidentCounter))
	print('Автоматически закрытых инцидентов: ' + str(autoclosedIncidentCounter))

	# отправка писем
	if savedincidents:
		emailsubst = structureIncidents(savedincidents)
		if emailsubst:
			message = fillEmailTemplate(incidentsNoticeTemplate, emailsubst)
			if message:
				header = 'Новый инцидент.'
				sendEmail(header, message)

else:
	# ежедневный отчет + проверка баланса
	parametersList = getParamCheckList()
	bushes = []
	for param_id in parametersList:
		a = resourceParameter(param_id)
		if a.parameterType == 1 and a.placeType == 1:
			bushes.append(param_id)

	date = date.today()
	a = dailyReport(date - timedelta(days = 1) )
	dailyReportPart = a.getReportMessage()

	balancePart = []
	for param_id in bushes:
		a = getIncidentRegisterDate(param_id, 'balance')
		if a['success']:
			if a['result']:
				last = a['result']
			else:
				last = date
		else: 
			last = date
		range = getDatesByDays(last, date)
		for dt in range:
			a = parameterBalance(param_id, dt[0])
			checkingDate = dt[0]
			b = a.checkBalanceAvailability(checkingDate)
			addr = ''
			if b['success']:
				checkingDate = dt[0] -  timedelta(days = 1)
				b = a.checkBalanceAvailability(checkingDate)
				if b['success']:
					b = a.getBalanceMessage()
					updateIncidentRegister(param_id, date, 'balance')
					if b == '':
						continue
					else:
						balancePart.append(b)
						a.last = {'lastChecked': dt[0]}
						balanceIncident = {'description': 'Небаланс', 'incidentType': 6, 'self': a}
						iHandler = incidentHandler()
						iHandler.saveIncident(balanceIncident)		
				else:
					addr = "<br>".join(str(x) for x in b['result'])
			else:
				addr = "<br>".join(str(x) for x in b['result'])
			if addr:
				balancePart.append('<span><strong>' + checkingDate.strftime("%Y-%m-%d") + ':<br>' + a.metadata['placeTypeName'] + ' ' + a.metadata['placeName'] + ':</strong> невозможно построить баланс. Нехватает данных по адресам:<br>' + addr +"</span>")

	balanceReportPart = ''
	footer = '<br><br><a href="http://pulsarweb.rsks.su:8080">Система мониторинга пульсар</a>'
	if len(balancePart) == 0:
		balanceReportPart = '<span>Отклонений по балансу за прошедший день не обнаружено.</span>'
	else:
		balanceReportPart = "<br><br>".join(str(x) for x in balancePart)

	message = dailyReportPart + balanceReportPart + footer
	header = 'Ежедневная сводка мониторинга за ' + str(date)
	sendEmail(header, message)						




