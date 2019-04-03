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

class controlledParameter():
	def __init__(self, id):
		self.cursor = conn.cursor()
		self.foundedIncidentsList = []
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
		self._placeCoord = False
		self._placeNameGroup = False
		
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
				self.loadcontrolledPlaceType()
				if self.controlledPlaceType == 1:
					self.checkBalanceFailure()
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
		args = (self._paramId_,)
		self.cursor = conn.cursor()
		try:
			self.cursor.execute(query, args)
			query = self.cursor.fetchone()
		except Exception as e:
			print(e)
			return False
		else:
			coords = 'https://static-maps.yandex.ru/1.x/?ll=_coords_&l=map&size=450,350&pt=_coords_,flag&z=12'

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
			
			if not query[12] == None:
				self._placeCoord = coords.replace('_coords_', query[12])
			else:
				self._placeCoord = 'https://tsc96.ru/upload/iblock/a5a/a5a129ed8c830e2dcafec7426d4c95d1.jpg'

			self._placeNameGroup = query[13]
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
			description = 'среднее значение не определено. Падение и повышение контроллировать невозможно!'	
		data = {
			'_lastArchiveTime':str(self._lastArchiveTime), 
			'_paramId_': self._paramId_, 
			'_paramName': self._paramName, 
			'_placeId': self._placeId, 
			'childPlace':  self._placeTypeName + ' ' + self._placeName,
			'parentPlace': self._parentPlaceTypeName + ' ' + self._parentPlaceName,
			'incidentType': lastIncidentType, 
			'description':description,
			'coordinates': self._placeCoord,
			'lastArchiveData': self._lastArchiveData,
			'placeNameGroup': self._placeNameGroup,
			'_lastArchiveTime': self._lastArchiveTime
			}
		self.foundedIncidentsList.append(data)
		#print(data)
		return data

	def checkBalanceFailure(self):
		if not self.initCompleted:
			return False
		else:
			if self.controlledPlaceType == 1:
				query = 'SELECT "Comment" FROM "Tepl"."PaternRep_cnt" WHERE pat_id = 6'
				try:
					self.cursor.execute(query)
					query = self.cursor.fetchone()
				except Exception as e:
					print(e)
					return False
				if query:
					print(query)
		return False
			



	def checkConnectionLost(self): #1
		if not self.initCompleted:
			return False
		else:
			if (datetime.now() - timedelta(hours = pollhourinterval + pollhourdelta)) > self._lastArchiveTime:
				self.dumpIncident(1)
			return False
		return True
		
	def checkConsumptionUp(self): #2
		if not self.initCompleted:
			return False
		else:
			if not (self._paramStartDate + timedelta(days = averageweekdays)) <= self._lastArchiveTime:
				return False
			else:
				range = getWeekDateRange(self._lastArchiveTime)
				self.averageWeek = self.getAverageValue(range)
				if not self.averageWeek == 0:
					if not self.averageWeek:
						self.dumpIncident(7)
						return True
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
				if not self.averageHour == 0:
					if not self.averageHour:
						self.dumpIncident(7)
						return True
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

	def getIncidents(self):
		if self.checkConnectionLost():
			return self.foundedIncidentsList
		else:
			if self.controlledParamType == 1:
				if self.checkConsumptionUp():
					return self.foundedIncidentsList
				else:
					if self.checkConsumptionStale():
						return self.foundedIncidentsList
			if self.controlledParamType == 2:
				if self.checkValueDown():
					return self.foundedIncidentsList
		return self.foundedIncidentsList

class incidentOperations():

	def autoCloseOpenedIncidents(self):
		a = 0
		self.cursor = conn.cursor()
		query = 'SELECT id, param_id, type FROM "Tepl"."Alert_cnt" WHERE status = \'active\' AND type = 1 '
		try:
			self.cursor.execute(query)
			query = self.cursor.fetchall()
		except Exception as e:
			print(e)
			return False
		else:
			for row in query:
				cp = controlledParameter(row[1])
				cl = cp.checkConnectionLost()
				if not cl:
					query = 'UPDATE "Tepl"."Alert_cnt" SET status = \'autoclosed\' WHERE id = %s '
					args = (row[0],)
					try:
						self.cursor.execute(query, args)
					except Exception as e:
						print(e)
						conn.rollback()
					else:
						conn.commit()
						a = a + 1
			print('autoclosed ' + str(a))

	def incidentExists(self,dumprecord):
		cursor = conn.cursor()
		query = 'SELECT param_id, type FROM "Tepl"."Alert_cnt" WHERE param_id = %s AND type = %s AND status = \'active\' '
		args = (dumprecord['_paramId_'], dumprecord['incidentType'])
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
		
	def saveIncidents(self, dump):
		a = 0
		savedIncidents = []
		if len(dump) > 0:
			cursor = conn.cursor()
			query = 'INSERT INTO "Tepl"."Alert_cnt"("time", param_id, type, param_name, place_id, "PARENT", "CHILD", description, staticmap, namegroup, lastarchivedata) \
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); '
			for dumprecord in dump:
				if not self.incidentExists(dumprecord):
					if dumprecord['_lastArchiveTime'] == False:
						d = date(2000, 1, 1)
						t = time(00, 00)
						dumprecord['_lastArchiveTime'] = datetime.combine(d, t)
					args = (
					dumprecord.get('_lastArchiveTime'),
					dumprecord.get('_paramId_'),
					dumprecord.get('incidentType'),
					dumprecord.get('_paramName'),
					dumprecord.get('_placeId'),
					dumprecord.get('parentPlace'),
					dumprecord.get('childPlace'),
					dumprecord.get('description'),
					dumprecord.get('coordinates'),
					dumprecord.get('placeNameGroup'),
					dumprecord.get('_lastArchiveData')
					)
					try:
						cursor.execute(query, args)
					except Exception as e:
						print(e)
						conn.rollback()
					else:
						conn.commit()
						a = a + 1
						savedIncidents.append(dumprecord)
			print('saved ' + str(a))
			return savedIncidents

class email():
	def __init__(self, dump):
		self.emailsubst = {}
		for dumprecord in dump:
			_parent = dumprecord.get('parentPlace')
			if _parent not in self.emailsubst:
				self.emailsubst[_parent] = {}
			_child = dumprecord.get('childPlace')
			if _child not in self.emailsubst[_parent]:
				self.emailsubst[_parent][_child] = []
			params = (dumprecord.get('_paramName'), dumprecord.get('description'))
			self.emailsubst[_parent][_child].append(params)

		for parent in self.emailsubst:
#			print(parent)
			for child in self.emailsubst[parent]:
#				print('=> ' + child)
				for pr in self.emailsubst[parent][child]:
					print('---> ' + child + ' ' + pr[0],pr[1])

	def send(self):
		if len(self.emailsubst) > 0:
			html = open(templatefile).read()
			template = Template(html)
			message = template.render(subst=self.emailsubst)
			recipients_emails = email_config['recipients_emails'].split(',')
			msg = MIMEText(message, 'html', 'utf-8')
			msg['Subject'] = Header('Новый инцидент.', 'utf-8')
			msg['From'] = "Система мониторинга Пульсар <pulsar@ce.int>"
			msg['To'] = ", ".join(recipients_emails)
			server = smtplib.SMTP(email_config['host'])
			server.sendmail(msg['From'], recipients_emails, msg.as_string())
			server.quit()
		print('emailed ' + str(len(self.emailsubst)))	

db_config = read_config('database')
email_config = read_config('email')

print(datetime.now())
print("Connecting to database...")
try:
	conn = psycopg2.connect(**db_config)
except psycopg2.OperationalError as e:
	print(e)
	print('Connection failed.')
	exit(0)
else:
	print('Connected!')

dump = []
io = incidentOperations()
io.autoCloseOpenedIncidents()

for id in getParamCheckList():
	cp = controlledParameter(id)
	arr = cp.getIncidents()
	if arr:
		for it in arr:
			dump.append(it)
print('finded ' +  str(len(dump)))
savedIncidentsDump = io.saveIncidents(dump)
em = email(savedIncidentsDump)
em.send()