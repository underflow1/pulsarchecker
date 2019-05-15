from functions_db import db
from parameterResource import parameterResource
from datetime import datetime, timedelta, date, time
import functions_stuff as stuff
from functions_config import config

class parameterIncidents(parameterResource):
	def __init__(self, id):
		parameterResource.__init__(self, id)
		self.balanceLackData = []
		self.date = None
		self.date_prev = None
		self.lastArchiveData = None
	
	def setDate(self, date):
		self.date = date
		self.date_prev = date - timedelta(days = 1)
		self.getCurrenArchiveValue()

	def getCurrenArchiveValue(self):
		query = ' SELECT "DataValue", "Delta" FROM "Tepl"."Arhiv_cnt" WHERE pr_id = $param_id AND typ_arh = 1 AND "DateValue" = $date '	
		args = {'param_id': self.param_id, 'date': self.date}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result:
			if self.parameterType == 1:
				self.lastArchiveData = round(result[1],2)
				return round(result[1],2)
			if self.parameterType == 2:
				self.lastArchiveData = round(result[0],2)
				return round(result[0],2)
			raise Exception('Этот тип параметра не учитывается')
		raise Exception('Архивное значение параметра не найдено')

	def getAverageValue(self, timerange):
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
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result:
			if result[0] < 0:
				raise Exception('Среднее значение имеет отрицательную величину')
			else:
				return result[0]
		else:
			raise Exception('Среднее значение не определено')

	def checkConsumptionUp(self):
		if self.date >= (self.metadata['paramStartDate'] + timedelta(days = config.averagedays)):
			timerange = stuff.getWeekAverageDateRange(self.date)
			averageValue = self.getAverageValue(timerange)
			currentValue = self.getCurrenArchiveValue()
			if currentValue > 0.5 and currentValue > (averageValue * 2):
				return True
		return False

	def checkConsumptionStale(self):
		if self.date >= (self.metadata['paramStartDate'] + timedelta(days = config.averagehours)):
			timerange = stuff.getHourAverageDateRange(self.date)
			averageValue = self.getAverageValue(timerange)
			if averageValue == 0:
				return True
		return False

	def checkValueDown(self):
		if self.date >= self.metadata['paramStartDate'] + timedelta(hours = config.averagehours):
			timerange = stuff.getHourAverageDateRange(self.date)
			averageValue = self.getAverageValue(timerange)
			if averageValue > self.getCurrenArchiveValue() * 2:
				return True
		return False

	def getCurrentIncident(self):
		if not self.initCompleted:
			return {'incidentType': 5, 'description': 'Параметр не инициализирован: ' + self.edescription, 'self': self}		
		else:
			if not self.connectionActive:
				return {'incidentType': 1, 'description': 'Прибор не вышел на связь в установленное время.', 'self': self}
			else:
				try:
					if self.parameterType == 1: # 1 - delta (volume)
						if self.checkConsumptionUp():
							return {'incidentType': 2, 'description': 'Зафиксировано повышение расхода контроллируемого параметра.', 'self': self}
						else:
							if self.checkConsumptionStale():
								return {'incidentType': 3, 'description': 'Зафиксировано отсутствие расхода.', 'self': self}
					if self.parameterType == 2: # 2 - value (pressure)
						if self.checkValueDown():
							return {'incidentType': 4, 'description': 'Зафиксировано падение значения параметра.', 'self': self}
				except Exception as e:
					return {'incidentType': 5, 'description': 'Параметр не инициализирован. Описание: ' + str(e), 'self': self}	
				else: 
					return False

	def getLackOfBalanceData(self, date):
		if self.placeType == 1:
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
			args = {'place_id': self.place_id, 'date': date}
			query = db.queryPrepare(query, args)
			result = db.fetchAll(query)
			if result:
				return False
			else:
				adresses  = []
				for item in result:
					adresses.append(item[0])
				return adresses

	def getBalanceAvailability(self):
		lack_curr = self.getLackOfBalanceData(self.date)
		lack_prev = self.getLackOfBalanceData(self.date_prev)
		place = self.metadata['placeTypeName'] + ' ' + self.metadata['placeName']
		if lack_curr:
			self.balanceLackData.append({'place': place, 'date': self.date, 'addresses': lack_curr})
		if lack_prev:
			self.balanceLackData.append({'place': place, 'date': self.date_prev, 'addresses': lack_prev})
		if len(self.balanceLackData) == 0:
			return True
		return False
		
	def getBalanceLackMessage(self):
		result = stuff.fillTemplate(config.balanceLackTemplate, self.balanceLackData)

	def getBalanceMessage(self):
		file = open(config.balanceQueryFile,'r')
		query = file.read()
		arguments = {'date_s': self.date_prev, 'date_e': self.date, 'place': self.place_id, 'type_a': 2}
		query = db.queryPrepare(query, arguments)
		result = db.fetchAll(query)
		balance = []
		if result:
			for item in result:
				if type(item).__name__ == 'float':
					item = round(item, 1)
				balance.append(item)
			place = self.metadata['placeTypeName'] + ' ' + self.metadata['placeName']
			self.balanceMessage = stuff.fillTemplate(config.balanceNoticeTemplate, {'place': place, 'balance': balance} )
			return self.balanceMessage	
		return None

