from functions_db import db

class parameterIncidents(resourceParameter):
	def __init__(self, id):
		resourceParameter.__init__(self, id)
#		self.dataLoaded = False
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

	def getCurrenArchiveValue(self, date):
		query = ' SELECT "DataValue", "Delta" FROM "Tepl"."Arhiv_cnt" WHERE pr_id = $param_id AND typ_arh = 1 AND "DateValue" = $date '	
		args = {'param_id': self.param_id, 'date': date}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result:
			if self.parameterType == 1:
				return round(result[1],2)
			if self.parameterType == 2:
				return round(result[0],2)
			raise Exception('Этот тип параметра не учитывается')
		raise Exception('Значение параметра не определено')

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
			return result[0]
		else:
			raise Exception('Среднее значение не определено')
