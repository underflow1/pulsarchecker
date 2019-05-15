from datetime import datetime, timedelta, date
from functions_db import db
from functions_config import config
from jinja2 import Template
from email.mime.text import MIMEText
from email.header import Header
import smtplib, time

# return: list ([0] - начало, [1] - конец)
def getWeekAverageDateRange(lastDate):
	datalist = []
	datalist.append(lastDate - timedelta(days = config.averagedays))
	datalist.append(lastDate - timedelta(days = 1))
	return datalist

# return: list ([0] - начало, [1] - конец)
def getHourAverageDateRange(lastDate):
	datalist = []
	datalist.append(lastDate - timedelta(hours = config.averagehours))
	datalist.append(lastDate)
	return datalist

def getDatesByHour(date_s, date_e):
	hour = timedelta(hours = 1)
	if not(date_s == date_e):
		if date_e - date_s < hour:
			raise Exception('Временной интервал меньше 1 часа')
		else:
			interval = []
			date = date_s
			while date <= date_e:
				interval.append(date)
				date = date + hour
			return interval
	return [date_s]
	
def getDatesByDays(date_s, date_e):
	day = timedelta(days = 1)
	if not(date_s == date_e):
		if date_e - date_s < day:
			raise Exception('Временной интервал меньше 1 дня')
		else:
			interval = []
			date = date_s
			while date <= date_e:
				interval.append(date)
				date = date + day
			return interval
	return [date_s]

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

def fillTemplate(templatefile, subst):
	if len(subst) > 0:
		html = open(templatefile).read()
		template = Template(html)
		message = template.render(subst=subst)
		return message

def sendEmail(header, message):
	recipients_emails = config.get('email', 'recipients_emails').split(',')
	msg = MIMEText(message, 'html', 'utf-8')
	msg['Subject'] = Header(header, 'utf-8')
	msg['From'] = "Система мониторинга <monitoring@rsks.su>"
	msg['To'] = ", ".join(recipients_emails)
	server = smtplib.SMTP(config.get('email', 'host'))
	server.sendmail(msg['From'], recipients_emails, msg.as_string())
	server.quit()		

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

def getDailyMessage(date):
	stats = {}
	query = ' SELECT COUNT(id) FROM "Tepl"."Alert_cnt" WHERE status = \'active\' '
	args = {'date': date}
	result = db.fetchAll(query)
	if result:
		stats['active'] = result[0]	

	query = ' SELECT COUNT(id) FROM "Tepl"."Alert_cnt" WHERE date(created_at) = $date '
	query = db.queryPrepare(query, args)
	result = db.fetchAll(query)
	if result:
		stats['created'] = result[0]	

	query = ' SELECT COUNT(id) FROM "Tepl"."Alert_cnt" WHERE status = \'autoclosed\' and date(updated_at) = $date '
	query = db.queryPrepare(query, args)
	result = db.fetchAll(query)
	if result:
		stats['autoclosed'] = result[0]			

	query = ' SELECT COUNT(id) FROM "Tepl"."Alert_cnt" WHERE status = \'closed\' and date(updated_at) = $date '
	query = db.queryPrepare(query, args)
	result = db.fetchAll(query)
	if result:
		stats['closed'] = result[0]			

	dailyReportMessage = fillTemplate(config.dailyReportNoticeTemplate, stats)
	return dailyReportMessage	

def printdots():
	print('\b.', end=' \b')
	time.sleep(0.3)
	print('.', end=' \b')
	time.sleep(0.2)
	print('.', end=' \b')
	time.sleep(0.1)
	print(' ОК.')
	time.sleep(0.3)

