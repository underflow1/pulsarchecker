from datetime import datetime, timedelta, date, time
from functions_db import db


class incidentHandler:
	def createIncident(self, incident):
		if len(incident) > 0:
			query = 'INSERT INTO "Tepl"."Alert_cnt"("time", param_id, type, status, param_name, place_id, "PARENT", "CHILD", description, staticmap, namegroup, lastarchivedata, lastchecked_time, is_completed) \
			VALUES ($time, $param_id, $type, $status, $param_name, $place_id, $PARENT, $CHILD", $description, $staticmap, $namegroup, $lastarchivedata, $lastchecked_time, $is_completed); '
			args = {
				'time': incident['self'].date,
				'param_id': incident['self'].param_id,
				'type': incident.incidentType,
				'status': 'active',
				'param_name': incident['self'].metadata['paramName'],
				'place_id': incident['self'].place_id,
				'PARENT': incident['self'].metadata['parentPlaceTypeName'] + ' ' + incident['self'].metadata['parentPlaceName'],
				'CHILD': incident['self'].metadata['placeTypeName'] + ' ' + incident['self'].metadata['placeName'],	
				'description': incident.description,
				'staticmap': incident['self'].metadata['placeCoord'],
				'namegroup': incident['self'].metadata['placeNameGroup'],
				'lastarchivedata': incident['self'].lastArchiveData,
				'lastchecked_time': incident['self'].date,
				'is_completed': False
			}
			query = db.queryPrepare(query, args)
			try:

				cursor.execute(query, args)
			except Exception as e:
				print(e)
				conn.rollback()
				return {'success': False, 'error': e, 'description':'Ошибка записи в базу данных'}
			else:
				conn.commit()
				return {'success': True, 'result': True}
