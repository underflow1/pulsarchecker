from datetime import datetime, timedelta, date, time
from functions_db import db
averagedays = 7
averagehours = 2

# return: list ([0] - начало, [1] - конец)
def getWeekAverageDateRange(lastDate):
	datalist = []
	datalist.append(lastDate - timedelta(days = averagedays))
	datalist.append(lastDate - timedelta(days = 1))
	return datalist

# return: list ([0] - начало, [1] - конец)
def getHourAverageDateRange(lastDate):
	datalist = []
	datalist.append(lastDate - timedelta(hours = averagehours))
	datalist.append(lastDate)
	return datalist

def getDatesByHour(date_s, date_e):
	hour = timedelta(hours = 1)
	if date_e - date_s < hour:
		raise Exception('Временной интервал меньше 1 часа')
	else:
		interval = []
		date = date_s
		while date <= date_e:
			interval.append(date)
			date = date + hour
		return interval

def getDatesByDays(date_s, date_e):
	day = timedelta(days = 1)
	if date_e - date_s < day:
		raise Exception('Временной интервал меньше 1 дня')
	else:
		interval = []
		date = date_s
		while date <= date_e:
			interval.append(date)
			date = date + day
		return interval

def getParamCheckList():
	query = ' SELECT prp_id FROM "Tepl"."Task_cnt" WHERE tsk_typ = 2 AND "Aktiv_tsk" =  True '
	result = db.fetchAll(query)
	if len(result) == 0:
		raise Exception('Нет ни одного параметра')
	else:
		params = []
		for item in result:
			params.append(item[0])
		return params

def updateIncidentRegister(param_id, date, regtype):
	query = ' SELECT count(*) FROM "Tepl"."Alerts_register" where param_id = $param_id and regtype= $regtype'
	args = {'param_id': param_id, 'regtype': regtype}
	query = db.queryPrepare(query, args)
	result = db.fetchAll(query)
	args = {'param_id': param_id, 'lastchecked_time': date, 'regtype': regtype}
	if result[0] == 0 :
		query = ' UPDATE "Tepl"."Alerts_register" SET lastchecked_time = $lastchecked_time WHERE param_id = $param_id and regtype= $regtype '
	else:
		query = ' INSERT INTO "Tepl"."Alerts_register"(param_id, lastchecked_time, regtype)  VALUES ($param_id, $lastchecked_time, $regtype) '
	db.executeInsertUpdate(query)

def getIncidentRegisterDate(param_id, regtype):
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