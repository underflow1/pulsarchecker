from datetime import datetime, timedelta, date, time
from functions_db import db

class incidentHandler:
	def createIncident(self, incident):
		query = 'INSERT INTO "Tepl"."Alert_cnt"("time", param_id, type, status, param_name, place_id, "PARENT", "CHILD", description, staticmap, namegroup, lastarchivedata, is_completed) \
		VALUES ($time, $param_id, $type, $status, $param_name, $place_id, $PARENT, $CHILD, $description, $staticmap, $namegroup, $lastarchivedata, $is_completed); '
		args = {
			'time': incident['self'].date,
			'param_id': incident['self'].param_id,
			'type': incident['incidentType'],
			'status': 'active',
			'param_name': incident['self'].metadata['paramName'],
			'place_id': incident['self'].metadata['placeId'],
			'PARENT': incident['self'].metadata['parentPlaceTypeName'] + ' ' + incident['self'].metadata['parentPlaceName'],
			'CHILD': incident['self'].metadata['placeTypeName'] + ' ' + incident['self'].metadata['placeName'],	
			'description': incident['description'],
			'staticmap': incident['self'].metadata['placeCoord'],
			'namegroup': incident['self'].metadata['placeNameGroup'],
			'lastarchivedata': incident['self'].lastArchiveData,
			'lastaverage': incident['self'].lastAverageValue,
			'is_completed': False
		}
		query = db.queryPrepare(query, args)
		db.executeInsertUpdate(query)

	def closeIncident(self, incident_id, close_type):
		if close_type == 1:
			status = 'autoclosed'
		else:
			status = 'closed'
		query = 'UPDATE "Tepl"."Alert_cnt" SET status = $status WHERE id = $incident_id '
		args = {'status': status, 'incident_id': incident_id}
		query = db.queryPrepare(query, args)
		db.executeInsertUpdate(query)

	def getExistingNotCompletedIncident(self, param_id, incident_type):
		query = 'SELECT MAX(id) FROM "Tepl"."Alert_cnt" WHERE status = \'active\' and param_id = $param_id and type = $incident_type and is_completed = False'
		args = {'param_id': param_id, 'incident_type': incident_type}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result:
			return result[0]
		return False

	def getOpenedNotCompletedIncidents(self, param_id):
		ids = []
		for incidentType in range (2,5):
			id = self.getExistingNotCompletedIncident(param_id, incidentType)
			if id:
				ids.append(id)
		if len(ids) > 0:
			return ids
		return False

	'''def getExistingIncidentLastCheckedTime(self, incident_id):
		query = 'SELECT lastchecked_time FROM "Tepl"."Alert_cnt" WHERE id = $incident_id'
		args = {'incident_id': incident_id}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result:
			return result[0]
		return False

	def updateExistingIncidentLastCheckedTime(self, incident_id, lastchecked_time):
		query = 'UPDATE "Tepl"."Alert_cnt" SET lastchecked_time = $lastchecked_time WHERE id = $incident_id '
		args = {'incident_id': incident_id, 'lastchecked_time': lastchecked_time}
		query = db.queryPrepare(query, args)
		db.executeInsertUpdate(query)'''

	def getExistingIncidentIsCompleted(self, incident_id):
		query = 'SELECT is_completed FROM "Tepl"."Alert_cnt" WHERE id = $incident_id'
		args = {'incident_id': incident_id}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result:
			return result[0]
		return False

	def updateExistingIncidentIsCompleted(self, incident_id):
		query = 'UPDATE "Tepl"."Alert_cnt" SET is_completed = TRUE WHERE id = $incident_id '
		args = {'incident_id': incident_id}
		query = db.queryPrepare(query, args)
		db.executeInsertUpdate(query)

	def getIncidentRegisterDate(self, param_id, regtype):
		query = ' SELECT lastchecked_time FROM "Tepl"."Alerts_register" where param_id = $param_id and regtype = $regtype '
		args = {'param_id': param_id, 'regtype': regtype}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		if result:
			if len(result) == 1:
				return result[0]
			else:
				raise Exception('Ошибка в регистре')
		return False

	def updateIncidentRegisterDate(self, param_id, date, regtype):
		query = ' SELECT count(*) FROM "Tepl"."Alerts_register" where param_id = $param_id and regtype = $regtype'
		args = {'param_id': param_id, 'regtype': regtype}
		query = db.queryPrepare(query, args)
		result = db.fetchAll(query)
		args = {'param_id': param_id, 'lastchecked_time': date, 'regtype': regtype}
		if result[0] == 0 :
			query = ' INSERT INTO "Tepl"."Alerts_register"(param_id, lastchecked_time, regtype)  VALUES ($param_id, $lastchecked_time, $regtype) '
		else:
			query = ' UPDATE "Tepl"."Alerts_register" SET lastchecked_time = $lastchecked_time WHERE param_id = $param_id and regtype= $regtype '
		query = db.queryPrepare(query, args)
		db.executeInsertUpdate(query)