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
		self.edescription = ''
		self.error = None
		a = self.loadStats()
		if a['success'] and a['result']:
			self.initCompleted = True
		else:
			self.error = a['error']
			self.edescription = a['description']

	def checkParameterExists(self):
		query = ' SELECT prp_id FROM "Tepl"."ParamResPlc_cnt" WHERE prp_id = %s '
		args = (self.param_id,)
		try:
			cursor.execute(query, args)
			query = cursor.fetchone()
		except Exception as e:
			print(e)
			return {'success': False, 'error': e, 'description':'Ошибка чтения базы данных'}
			exit(0)
		else:
			if query:
				if query[0] == self.param_id:
					return {'success': True, 'result': True}
			return {'success': False, 'error': True, 'description':'Такого параметра не существует'}

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
			exit(0)
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
			a = self.getLastArchive()
			if a['success'] and a['result']:
				self.last = a['result']
				self.dataLoaded = True
			else:
				self.error = a['error']
				self.edescription = a['description']	

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
				exit(0)
			else:	
				if query[0]:
					return {'success': True, 'result': query[0]}
				return {'success': False, 'error': True, 'description': 'Последняя дата не определена'}
		else:
			return {'success': False, 'error': self.error, 'description': self.edescription}

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
					exit(0)
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
			return {'success': False, 'error': self.error, 'description': self.edescription}
	
	def checkConnectionLost(self): #1
		if self.dataLoaded:
			if (datetime.now() - timedelta(hours = pollhourinterval + pollhourdelta)) > self.last['time']:
				return {'success': True, 'result': True}
			else:
				return {'success': True, 'result': False}
		else:
			return {'success': False, 'error': self.error, 'description': self.edescription}
		
	def getAverageValue(self, timerange):
		if self.dataLoaded:
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
				exit(0)
			else:	
				if query[0]:
					return {'success': True, 'result': query[0]}
				return {'success': False, 'error': True, 'description': 'Среднее значение не определено'}
		else:
			return {'success': False, 'error': self.error, 'description': self.edescription}

	def checkConsumptionUp(self):
		if self.dataLoaded:
			if not self.last['time'] >= (self.metadata['paramStartDate'] + timedelta(days = averageweekdays)):
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
		else:
			return {'success': False, 'error': self.error, 'description': self.edescription}

	def checkConsumptionStale(self):
		if self.dataLoaded:
			if not self.last['time'] >= (self.metadata['paramStartDate'] + timedelta(hours = pollhourinterval)):
				return {'success': False, 'error': True, 'description': "С начала сбора данных прошло слишком мало времени. Определить среднее значение невозможно" }
			else:
				range = getHourDateRange(self.last['time'])
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

	def checkValueDown(self):
		if self.dataLoaded:
			if not  self.last['time'] >= (self.metadata['paramStartDate'] + timedelta(hours = pollhourinterval)):
				return {'success': False, 'error': True, 'description': "С начала сбора данных прошло слишком мало времени. Определить среднее значение невозможно" }
			else:
				range = getHourDateRange(self.last['time'])
				a = self.getAverageValue(range)
				if a['success']:
					averageValue = a['result']
					if (averageValue > self.last['value'] * 2):
						return {'success': True, 'result': True}
					else:
						return {'success': True, 'result': False}
				else:
					return {'success': False, 'error': a['error'], 'description': a['description'] }					
		else:
			return {'success': False, 'error': self.error, 'description': self.edescription}

	def getCurrentIncident(self):
		data = self
		if not self.initCompleted:
			return {'success': True, 'result': {'incidentType': 5, 'description': 'Параметр не инициализирован.', 'self': data}}
		else: 
			if not self.dataLoaded:
				return {'success': True, 'result': {'incidentType': 5, 'description': 'Параметр не инициализирован.', 'self': data}}
			else:
				inc = self.checkConnectionLost()
				if (inc['success'] and inc['result']):
					return {'success': True, 'result': {'incidentType': 1, 'description': 'Прибор не вышел на связь в установленное время.', 'self': data}}
				else:
					if self.parameterType == 1: # 1 - delta (volume)
						inc = self.checkConsumptionUp()	
						if (inc['success'] and inc['result']):
							return {'success': True, 'result': {'incidentType': 2, 'description': 'Зафиксировано повышение расхода контроллируемого параметра.', 'self': data}}

						else:
							inc = self.checkConsumptionStale()
							if (inc['success'] and inc['result']):
								return {'success': True, 'result': {'incidentType': 3, 'description': 'Зафиксировано отсутствие расхода.', 'self': data}}
					if self.parameterType == 2: # 2 - value (pressure)
						inc = self.checkConsumptionUp()
						if (inc['success'] and inc['result']):
							return {'success': True, 'result': {'incidentType': 4, 'description': 'Зафиксировано падение значения параметра.', 'self': data}}
		return {'success': True, 'result': None}

class incidentHandler:
	def saveIncident(self, incident):
		pass
		if len(incident) > 0:
			query = 'INSERT INTO "Tepl"."Alert_cnt"("time", param_id, type, param_name, place_id, "PARENT", "CHILD", description, staticmap, namegroup, lastarchivedata) \
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); '
			if not incident['self'].last.get('time'):
				d = date(2000, 1, 1)
				t = time(00, 00)
				incident['self'].last['time'] = datetime.combine(d, t)
			args = (
			incident['self'].last['time'],
			incident['self'].param_id,
			incident['incidentType'],
			incident['self'].metadata['paramName'],
			incident['self'].metadata['placeId'],
			incident['self'].metadata['parentPlaceTypeName'] + ' ' + incident['self'].metadata['parentPlaceName'],
			incident['self'].metadata['placeTypeName'] + ' ' + incident['self'].metadata['placeName'],			
			incident['description'] + ' ' + incident['self'].edescription,
			incident['self'].metadata['placeCoord'],
			incident['self'].metadata['placeNameGroup'],
			incident['self'].last.get('value')
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
		query = 'SELECT id FROM "Tepl"."Alert_cnt" WHERE status = \'active\' and param_id = %s and type = %s'
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

def fillEmailTemplate(emailsubst):
	if len(emailsubst) > 0:
		html = open(templatefile).read()
		template = Template(html)
		message = template.render(subst=emailsubst)
		return message

def sendIncidentsNotice(message):
		recipients_emails = email_config['recipients_emails'].split(',')
		msg = MIMEText(message, 'html', 'utf-8')
		msg['Subject'] = Header('Новый инцидент.', 'utf-8')
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
parametersList = getParamCheckList()
for param_id in parametersList:
	iHandler = incidentHandler()
	# проверка наличия открытого инцидента "не выход на связь"
	a = iHandler.getExistingIncident(param_id, 1)
	if a['success'] and a['result']:
		lostConnectionIncident = a['result']
	else:
		lostConnectionIncident = None
	# проверка и создание новых инцидентов
	pIncident = parameterIncidents(param_id)
	a = pIncident.getCurrentIncident()
	if a['success']:
		if a['result']:
			incident = a['result']
			a = iHandler.getExistingIncident(param_id, incident['incidentType'])
			if not (a['success'] and a['result']):
				a = iHandler.saveIncident(incident)
				if a['success'] and a['result']:
					savedIncidentCounter += 1
					savedincidents.append(incident)
		else: 
			incident = None
	# автозакрытие "не выход на связь"
	if lostConnectionIncident:
		if incident:
			if incident['incidentType'] > 1:
				iHandler.closeIncident(lostConnectionIncident, 1)
				autoclosedIncidentCounter += 1
		else:
			iHandler.closeIncident(lostConnectionIncident, 1)
			autoclosedIncidentCounter += 1
print('Новых инцидентов: ' + str(savedIncidentCounter))
print('Автоматически закрытых инцидентов: ' + str(autoclosedIncidentCounter))

if savedincidents:
	emailsubst = structureIncidents(savedincidents)
	if emailsubst:
		message = fillEmailTemplate(emailsubst)
		if message:
			sendIncidentsNotice(message)
pass
