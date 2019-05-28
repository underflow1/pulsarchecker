from parameterIncidents import parameterIncidents
from handlerIncidents import incidentHandler
from datetime import datetime, timedelta, date
import functions_stuff as stuff
import os, sys, time
from functions_config import config

autoclosedIncidentCounter = 0
is_completedCounter = 0
createdIncidents = []

if len(sys.argv) != 1:
	parametersList = stuff.getParamCheckList()
	for param_id in parametersList:
		iHandler = incidentHandler()
		pIncident = parameterIncidents(param_id)
		print('Параметр:', param_id, pIncident.metadata['parentPlaceName'], pIncident.metadata['placeName'])
		activeLostIncident = False
		activeOtherIncident = False
		activeOtherIncidentTime = False
		activeLostIncidentTime = False

		# Проверка связи
		connectionLost = not pIncident.connectionActive
		# Проверка наличия активного инцидента connection lost
		connectionLostIncident = iHandler.getExistingNotCompletedIncident(param_id, 1)

		if connectionLost: # если связь потеряна
			print('Связь потеряна')
			if not connectionLostIncident:  # а инцидента нет, то создать инцидент и смотрим следующий
				print('Активного инцидента не выход на связь нет. Создать.', end = ' \b ')
				currentIncident = pIncident.getCurrentIncident()
				iHandler.createIncident(currentIncident)
				createdIncidents.append(currentIncident)
				stuff.printdots()
				continue 
			else:  # и инцидент уже есть, то пропускаем этот параметр и смотрим следующий
				continue
		else: # если связь не потеряна
			print('Прибор на связи.', end = ' \b ')
			if connectionLostIncident: # а инцидент есть, то его надо закрыть: 
				print('Есть активный инцидент не выход на связь. Закрыть', end = ' \b ')
				iHandler.closeIncident(connectionLostIncident, 1)
				autoclosedIncidentCounter += 1
				stuff.printdots()
		# далее мы строим интервал пропущенных проверок для данного параметра 
		newestDate = pIncident.newestArchiveTime
		lastCheckDate = iHandler.getIncidentRegisterDate(param_id, 'incident')
		if lastCheckDate == newestDate:
			print('Пропущеных точек нет.')
			iHandler.updateIncidentRegisterDate(param_id, newestDate, 'incident')
		else:
			if not lastCheckDate:
				dates = [newestDate]
			else:
				dates = stuff.getDatesByHour(lastCheckDate, newestDate)
			print('Количество пропущеных точек:', len(dates))
			# и начинаем проверять каждую дату на наличие инцидента (кроме потеря связи)
			for date in dates:
				pIncident.setDate(date) # устанавливаем дату
				currentIncident = pIncident.getCurrentIncident() # чекаем инцидент
				openedNotCompletedIncidentList = iHandler.getOpenedNotCompletedIncidents(param_id) # загружаем список открытых не завершенных инцидентов
				if not currentIncident: # если в эту дату инцидента нет
					print(str(date), end = ' \b ')
					if not openedNotCompletedIncidentList: # а так же нет никаких открытых незавершенных инцидентов, то смотрим следущую дату
						print('Инцидентов нет', end = ' \r ')
						continue
					else: # если есть открытые не завершенные инциденты, то их надо все завершить (is_completed)
						print('Обнаружены открытые незавершенные инциденты. Завершить', end = ' \b ')
						for id in openedNotCompletedIncidentList:
							iHandler.updateExistingIncidentIsCompleted(id)
							is_completedCounter += 1
						stuff.printdots()
				else: # если все-таки в эту дату нашелся инцидент
					print(str(date), 'Обнаружен инцидент тип', currentIncident['incidentType'], end = ' \b ')
					# проверяем наличие такого же незавершенного инцидента
					openedNotCompletedIncidentId = iHandler.getExistingNotCompletedIncident(param_id, currentIncident['incidentType'])
					if not openedNotCompletedIncidentId: # если незавершенного инцидента нет, то создаём инцидент
						print('Инцидент новый - cоздать', end = ' \b ')
						iHandler.createIncident(currentIncident)
						createdIncidents.append(currentIncident)
						stuff.printdots()
					else: # а если есть не завершенный открытый инцидент такого же типа, то это он и есть (т.е. инцидент продолжается) и мы смотрим следущую дату
						print('- продолжение старого инцидента', end = ' \r ')
						continue
			iHandler.updateIncidentRegisterDate(param_id, date, 'incident')
			print()
	print()
	print('Новых инцидентов:', len(createdIncidents))
	print('Завершенных инцидентов:', is_completedCounter)
	print('Автоматически закрытых инцидентов:', autoclosedIncidentCounter)
	# отправка писем
	if createdIncidents:
		emailsubst = stuff.structureIncidents(createdIncidents)
		if emailsubst:
			message = stuff.fillTemplate(config.incidentsNoticeTemplate, emailsubst)
			if message:
				message = message + stuff.returnFooter()
				header = 'Новый инцидент.'
				stuff.sendEmail(header, message)

else:# ежедневный отчет + проверка баланса
	if len(sys.argv) == 1:# and sys.argv[1] == 'balance':
		parametersList = stuff.getParamCheckList()
		bushes = []
		for param_id in parametersList:
			pIncident = parameterIncidents(param_id)
			if pIncident.parameterType == 1 and pIncident.placeType == 1:
				bushes.append(param_id)

		balanceMessage = ''
		
		for param_id in bushes:
			iHandler = incidentHandler()
			pBush = parameterIncidents(param_id)
			balanceDate = datetime.combine(date.today(), datetime.min.time()) - timedelta(days = 1)
			balanceLastCheckDate = iHandler.getIncidentRegisterDate(param_id, 'balance')
			bbdate = None
			print('placeid:', pBush.metadata['placeId'], pBush.metadata['placeTypeName'], pBush.metadata['placeName'], end = ' \b')
			if balanceDate == balanceLastCheckDate:
				print(' Пропущеных балансов нет.')
			else:
				if not balanceLastCheckDate:
					dates = [balanceDate]
				else:
					dates = stuff.getDatesByDays(balanceLastCheckDate, balanceDate)
				print(' пропущено дней баланса:', len(dates))
				# и начинаем проверять баланс на каждую из пропущенных дат
				balancePart = ''
				for bdate in dates:
					try:
						pBush.setDate(bdate)
						bp = pBush.getBalanceMessage()
					except:
						balancePart = balancePart + bdate.strftime("%Y%m%d") + ' ошибка<br>'
						break
					else:
						if bp:
							balancePart = balancePart + bp			
				if balancePart:
					balanceMessage = balanceMessage + '<strong><h5>' + pBush.metadata['parentPlaceName'] + ' ' + pBush.metadata['placeName'] + '</h5></strong>' + balancePart
		
		reportDate = date.today() - timedelta(days = 1)
		dailyMessage = stuff.getDailyMessage(reportDate)
		if len(balanceMessage) == 0:
			balanceMessage = '<span>Отклонений по балансу не обнаружено.</span>'
		message = dailyMessage + balanceMessage + stuff.returnFooter()
		header = 'Ежедневная сводка мониторинга за ' + str(reportDate)
		stuff.sendEmail(header, message)
	else:
		print('Неверный вызов')