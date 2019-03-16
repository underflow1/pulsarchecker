import psycopg2, time, os, json, smtplib
from datetime import datetime, timedelta, date
from jinja2 import Template
from configparser import ConfigParser
from email.mime.text import MIMEText
from email.header    import Header
from configreader import read_config

db_config = read_config('database')
email_config = read_config('email')
print("Connecting to database...")
time.sleep(1)
try:
	conn = psycopg2.connect(**db_config)
except psycopg2.OperationalError as e:
	print(e)
	print('Connection failed.')
	exit(0)
else:
	print('Connected!')

# !временно инициализация параметров
averageweekdays = 7 # для ретроспективного вычисления среднего значения в определенный час 
pollhourinterval = 2 # интервал выхода на связь пульсаров (в часах) используется при вычислении среднего
pollhourdelta = 2 # лаг добавляемый при проверке (в часах)

class controlledParameter():
	def __init__(self, id):
		self.cursor = conn.cursor()
		self.incidentslist = []
		self._paramId_ = id				# значения начинаюищеся с подчеркивания - те что мы берем из базы
		self._paramTypeId = False		# значения без подчеркивания - вычисленные в ходе инициализации
		self._paramName = False
		self._paramStartDate = False
		self._paramNameGroup = False
		self._placeId = False
		self._placeTypeId = False
		self._placeTypeName = False
		self._placeName = False
		self._parentPlaceId = False
		self._parentPlaceTypeId = False
		self._parentPlaceTypeName = False
		self._parentPlaceName = False
		
		self.controlledParamType = False # 1 - delta (volume), 2 - value (pressure)
		self.controlledPlaceType = False # в дальнейшем для инцидентов на основе типа места (например, для баланса по кусту)
		self._lastArchiveTime = False
		self._lastArchiveData = False

		self.averageWeek = False
		self.averageHour = False
		self.initCompleted = False

		if self.checkParameterExists():
			if not self.loadParameterMetadata():
				self.dumpIncident(6)
			else:
				if not self.loadcontrolledParamType():
					self.dumpIncident(0)
				else:
					if not self.loadlastArchiveTime():
						self.dumpIncident(5)
					else:
						if not self.loadlastArchiveData():
							self.dumpIncident(5)
						else:
							self.initCompleted = True
	
	def checkParameterExists(self):
		query = ' SELECT prp_id FROM "Tepl"."ParamResPlc_cnt" WHERE prp_id = %s '
		args = (self._paramId_,)
		try:
			self.cursor.execute(query, args)
			query = self.cursor.fetchone()
		except Exception as e:
			print(e)
			return False
		else:
			if query:
				return True
			return False

	def loadParameterMetadata(self):
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
				task."DateStart" as _paramStartDate \
		FROM "Tepl"."ParamResPlc_cnt" paramlist \
		LEFT JOIN "Tepl"."ParametrResourse" paramres on paramlist."ParamRes_id" = paramres."ParamRes_id" \
		LEFT JOIN "Tepl"."Places_cnt" place on paramlist.plc_id = place.plc_id \
		LEFT JOIN "Tepl"."PlaceTyp_cnt" placetype on place.typ_id = placetype.typ_id  \
		LEFT JOIN "Tepl"."Places_cnt" parentplace on place.place_id = parentplace.plc_id \
		LEFT JOIN "Tepl"."PlaceTyp_cnt" parentplacetype on parentplace.typ_id = parentplacetype.typ_id \
		LEFT JOIN (SELECT * FROM "Tepl"."Task_cnt" WHERE tsk_typ = 2 AND "Aktiv_tsk" =  True) task on paramlist.prp_id = task.prp_id \
		WHERE paramlist.prp_id = %s'
		args = (self._paramId_,)
		self.cursor = conn.cursor()
		try:
			self.cursor.execute(query, args)
			query = self.cursor.fetchone()
		except Exception as e:
			print(e)
			return False
		else:
			self._paramTypeId = query[1]
			self._paramName = query[2]
			self._placeId = query[3]
			self._placeName = query[4]
			self._placeTypeId = query[5]
			self._placeTypeName = query[6]
			self._parentPlaceId = query[7]
			self._parentPlaceName = query[8]
			self._parentPlaceTypeId = query[9]
			self._parentPlaceTypeName = query[10]
			self._paramStartDate = query[11].replace(tzinfo=None)
			return True

	def loadcontrolledParamType(self):
		if self._paramTypeId in (1,):
			self.controlledParamType = 1
			return True
		if self._paramTypeId in (269, 308): 
			self.controlledParamType = 2
			return True
		return False

	def loadcontrolledPlaceType(self):
		if self._placeTypeId in (20,):
			self.controlledPlaceType = 1
			return True
		return False

	def loadlastArchiveTime(self):
		query = ' SELECT MAX("DateValue") FROM "Tepl"."Arhiv_cnt" WHERE pr_id = %s AND typ_arh = 1'
		args = (self._paramId_,)
		try:
			self.cursor.execute(query, args)
			query = self.cursor.fetchone()
		except Exception as e:
			print(e)
			return False
		else:	
			if query[0]:
				self._lastArchiveTime = query[0]	
				return True
			else:
				return False
		
	def loadlastArchiveData(self):
		query = ' \
		SELECT "DataValue", "Delta" FROM "Tepl"."Arhiv_cnt" \
		WHERE pr_id = %s AND typ_arh = 1 \
		AND "DateValue" = %s '
		args = (self._paramId_,self._lastArchiveTime)
		try:
			self.cursor.execute(query, args)
			query = self.cursor.fetchone()
		except Exception as e:
			print(e)
			return False
		else:	
			if query:	
				if self.controlledParamType == 1:
					self._lastArchiveData = round(query[1],2) 
					return True
				if self.controlledParamType == 2: 
					self._lastArchiveData = round(query[0],2)
					return True
			return False

	def getAverageValue(self, range):
		if range[1] - range[0] > timedelta(hours = 24):
			rangetype = '1 day'
		else:
			rangetype = '1 hour'
		for item in range:
			item = str(item)
		query = '\
		DROP TABLE IF EXISTS date_range_tmp; \
		CREATE TEMPORARY TABLE date_range_tmp("DateValue" timestamp without time zone); \
		INSERT INTO date_range_tmp SELECT "Tepl"."GetDateRange"(%s, %s, %s);\
		SELECT * FROM date_range_tmp;\
		SELECT SUM(CASE WHEN %s = 1 THEN "Delta" ELSE "DataValue" END)/(SELECT COUNT(*) FROM date_range_tmp) FROM "Tepl"."Arhiv_cnt"\
		WHERE pr_id = %s AND typ_arh = 1 AND "DateValue" IN (SELECT * FROM date_range_tmp);'
		args = (range[0], range[1], rangetype, self.controlledParamType, self._paramId_)
		try:
			self.cursor.execute(query, args)
			query = self.cursor.fetchone()
		except Exception as e:
			print(e)
			return False
		else:	
			return query[0]

	def dumpIncident(self,lastIncidentType):
		description = 'НЕИЗВЕСТНЫЙ ИНЦИДЕНТ'
		if lastIncidentType == 0:
			description = 'Данный тип параметра не контроллируется'
		if lastIncidentType == 1:
			description = 'Прибор не вышел на связь в установленное время'
		if lastIncidentType == 2:
			description = 'Зафиксировано повышение контроллируемого параметра более 100%'
		if lastIncidentType == 3:
			description = 'Зафиксировано отсутствие расхода'
		if lastIncidentType == 4:
			description = 'Зафиксировано падение контроллируемого параметра более 50%'									
		if lastIncidentType == 5:
			description = 'Отсутствуют архивные данные'			
		if lastIncidentType == 6:
			description = 'Отсутствуют метаданные параметра'	
		if lastIncidentType == 7:
			description = 'Невозможно определить среднее значение контроллируемого параметра'	
		data = {
			'_lastArchiveTime':str(self._lastArchiveTime), 
			'_paramId_': self._paramId_, 
			'_paramName': self._paramName, 
			'_placeId': self._placeId, 
			'childPlace':  self._placeTypeName + self._placeName,
			'parentPlace': self._parentPlaceTypeName + self._parentPlaceName,
			'incidentType': lastIncidentType, 
			'description':description
			}
		print(data)
		return data

	def checkConnectionLost(self): #1
		if not self.initCompleted:
			return False
		else:
			if (datetime.now() - timedelta(hours = pollhourinterval + pollhourdelta)) > self._lastArchiveTime:
				self.incidentslist.append(self.dumpIncident(1))
			return False
		
	def checkConsumptionUp(self): #2
		if not self.initCompleted:
			return False
		else:
			if not (self._paramStartDate + timedelta(days = averageweekdays)) <= self._lastArchiveTime:
				return False
			else:
				range = getWeekDateRange(self._lastArchiveTime)
				self.averageWeek = self.getAverageValue(range)
				if not self.averageWeek:
					self.dumpIncident(7)
					return False
				else:
					if (self.averageWeek * 2) < self._lastArchiveData:
						self.dumpIncident(2)
						return True
					return False

	def checkConsumptionStale(self): #3
		if not self.initCompleted:
			return False
		else:
			if not (self._paramStartDate + timedelta(hours = pollhourinterval)) <= self._lastArchiveTime:
				return False
			else:
				range = getHourDateRange(self._lastArchiveTime)
				self.averageHour = self.getAverageValue(range)
				if not self.averageHour:
					self.dumpIncident(7)
					return False
				else:
					if self.averageHour == 0:
						self.dumpIncident(3)
						return True
					return False

	def checkValueDown(self): #4
		if not self.initCompleted:
			return False
		else:
			if not (self._paramStartDate + timedelta(hours = pollhourinterval)) <= self._lastArchiveTime:
				return False
			else:
				range = getHourDateRange(self._lastArchiveTime)
				self.averageHour = self.getAverageValue(range)
				if not self.averageHour:
					self.dumpIncident(7)
				else:
					if (self.averageHour >  self._lastArchiveData * 2):
						self.dumpIncident(4)
						return True
					else:
						return False

class incident():
	def getIncident(self):
		data = []
		if json.loads(self.checkConnectionLost())['result']:
			data.append(self.dump())
		else: 
			if self.controlledParameter.datatype == 1: 
				if json.loads(self.checkConsumptionUp())['result']:
					data.append(self.dump())
				else:
					if json.loads(self.checkConsumptionStale())['result']:
						data.append(self.dump())
					else:
						return json.dumps({'result':False})
			if self.controlledParameter.datatype == 2: 
				if json.loads(self.checkValueDown())['result']:
					data.append(self.dump())
				else: 
					return json.dumps({'result':False})

		return json.dumps(
				{'result':True, 
				'data':data
				})
	

def sendEmail(message):
	recipients_emails = email_config['recipients_emails'].split(',')
	msg = MIMEText(message, 'html', 'utf-8')
	msg['Subject'] = Header('Новый инцидент.', 'utf-8')
	msg['From'] = "Система мониторинга Пульсар <pulsar@ce.int>"
	msg['To'] = ", ".join(recipients_emails)
	server = smtplib.SMTP(email_config['HOST'])
	server.sendmail(msg['From'], recipients_emails, msg.as_string())
	server.quit()

def incidentExists(dump):
	cursor = conn.cursor()
	query = 'SELECT param_id, type FROM "Tepl"."Alert_cnt" WHERE param_id = %s AND type = %s AND status = \'active\' '
	args = (dump.get('param_id'),dump.get('type'))
	try:
		cursor.execute(query, args)
		query = cursor.fetchall()
	except Exception as e:
		print(e)
		return False
	else:
		if len(query) > 0:
			return True
	return False
	

def autoCloseIncidents():
	cursor = conn.cursor()
	query = 'SELECT id, param_id, type FROM "Tepl"."Alert_cnt" WHERE status = \'active\' '
	try:
		cursor.execute(query)
		query = cursor.fetchall()
	except Exception as e:
		print(e)
		return False
	activeincidents = []
	closedincidents = []
	for row in query:
		ainc = {}
		ainc['id'] = row[0]
		ainc['param_id'] = row[1]
		ainc['type'] = row[2]
		activeincidents.append(ainc)
	for item in activeincidents:
		if item['type'] == 1: # автозакрываем только невыходы на связь
			parameter = controlledParameter(item['param_id'])
			autoclosecandidate = incident(parameter)
			if not json.loads(autoclosecandidate.checkConnectionLost())['result']:
				query = 'UPDATE "Tepl"."Alert_cnt" SET status = \'autoclosed\' WHERE id = %s '
				args = (item['id'],)
				try:
					cursor.execute(query, args)
				except Exception as e:
					print(e)
					conn.rollback()
				else:
					conn.commit()	
					closedincidents.append(item)		
	if len(closedincidents) > 0:
		return closedincidents
	return False

def saveIncidents(newincidents):
	if len(newincidents) > 0:
		cursor = conn.cursor()
		query = 'INSERT INTO "Tepl"."Alert_cnt"("time", param_id, type, param_name, place_id, "PARENT", "CHILD", description) \
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s); '
		for newinc in newincidents:
			if not newinc.get('time'):
				d = date(2000, 1, 1)
				t = time(00, 00)
				time1 = datetime.combine(d, t)
			else:
				time1 = newinc.get('time')
			args = (
			time1,
			newinc.get('param_id'),
			newinc.get('type'),
			newinc.get('param_name'),
			newinc.get('place_id'),
			newinc.get('PARENT'),
			newinc.get('CHILD'),
			newinc.get('description'))
			try:
				cursor.execute(query, args)
			except Exception as e:
				print(e)
				conn.rollback()
			else:
				conn.commit()

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

a = controlledParameter(90)
print(a.checkConnectionLost())
print(a.checkConsumptionUp())
print(a.checkConsumptionStale())
print(a.checkValueDown())