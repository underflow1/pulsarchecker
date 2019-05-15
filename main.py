from parameterIncidents import parameterIncidents
from handlerIncidents import incidentHandler
from datetime import datetime, timedelta
import functions_stuff as stuff
import os, sys, time

autoclosedIncidentCounter = 0
createdIncidents = []

if len(sys.argv) == 1:
	parametersList = stuff.getParamCheckList()
	for param_id in parametersList:
		iHandler = incidentHandler()
		pIncident = parameterIncidents(param_id)
		print('Параметр:', param_id, pIncident.metadata['parentPlaceName'], pIncident.metadata['placeName'])
		time.sleep(0.1)
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
				print('Активного инцидента не выход на связь нет. Создать.')
				currentIncident = pIncident.getCurrentIncident()
				iHandler.createIncident(currentIncident)
				createdIncidents.append(currentIncident)
				print('Инцидент не выход на связь создан.')
				continue 
			else:  # и инцидент уже есть, то пропускаем этот параметр и смотрим следующий
				continue
		else: # если связь не потеряна
			print('Прибор на связи.', end = ' \b ')
			time.sleep(0.1)
			if connectionLostIncident: # а инцидент есть, то его надо закрыть: 
				print('Есть активный инцидент не выход на связь. Закрыть')
				iHandler.closeIncident(connectionLostIncident, 1)
				autoclosedIncidentCounter += 1
				print('Инцидент не выход на связь закрыт.')

		# далее мы строим интервал пропущенных проверок для данного параметра 
		newestDate = pIncident.newestArchiveTime
		lastCheckDate = iHandler.getIncidentRegisterDate(param_id, 'incident')
		if not lastCheckDate:
#			lastCheckDate = newestDate
#		if (newestDate == lastCheckDate):
			print('Пропущеных точек нет.')
			iHandler.updateIncidentRegisterDate(param_id, newestDate, 'incident')
		else:
			dates = stuff.getDatesByHour(lastCheckDate, newestDate)
			print('Пропущено:', len(dates), 'точек.')
			# и начинаем проверять каждую дату на наличие инцидента (кроме потеря связи)
			for date in dates:
				pIncident.setDate(date) # устанавливаем дату
				currentIncident = pIncident.getCurrentIncident() # чекаем инцидент
				openedNotCompletedIncidentList = iHandler.getOpenedNotCompletedIncidents(param_id) # загружаем список открытых не завершенных инцидентов
				if not currentIncident: # если в эту дату инцидента нет
					print(str(date), end = ' \b ')
					if not openedNotCompletedIncidentList: # а так же нет никаких открытых незавершенных инцидентов, то смотрим следущую дату
						print('Инцидентов нет')
						continue
					else: # если есть открытые не завершенные инциденты, то их надо все завершить (is_completed)
						print('Обнаружены открытые незавершенные инциденты. Завершить.', end = ' \b ')
						time.sleep(0.1)
						for id in openedNotCompletedIncidentList:
							iHandler.updateExistingIncidentIsCompleted(id)
						print('Завершены.')
						time.sleep(1)
				else: # если все-таки в эту дату нашелся инцидент
					print(str(date), 'Обнаружен инцидент тип', currentIncident['incidentType'], end = ' \b ')
					time.sleep(0.1)
					# проверяем наличие такого же незавершенного инцидента
					openedNotCompletedIncidentId = iHandler.getExistingNotCompletedIncident(param_id, currentIncident['incidentType'])
					if not openedNotCompletedIncidentId: # если незавершенного инцидента нет, то создаём инцидент
						print('Инцидент новый - cоздать.', end = ' \b ')
						time.sleep(0.1)
						iHandler.createIncident(currentIncident)
						createdIncidents.append(currentIncident)
						print('Создано')
						time.sleep(1)
					else: # а если есть не завершенный открытый инцидент такого же типа, то это он и есть (т.е. инцидент продолжается) и мы смотрим следущую дату
						print('- продолжение старого инцидента', end = ' \r ')
						continue
			iHandler.updateIncidentRegisterDate(param_id, date, 'incident')

	print('Новых инцидентов:', len(createdIncidents))
	print('Автоматически закрытых инцидентов:', autoclosedIncidentCounter)
	# отправка писем
	if savedincidents:
		emailsubst = structureIncidents(savedincidents)
		if emailsubst:
			message = stuff.fillTemplate(incidentsNoticeTemplate, emailsubst)
			if message:
				header = 'Новый инцидент.'
				stuff.sendEmail(header, message)

else:# ежедневный отчет + проверка баланса
	if len(sys.argv) ==2 and sys.argv[1] == 'balance':
		parametersList = getParamCheckList()
		bushes = []
		for param_id in parametersList:
			pIncident = parameterIncidents(param_id)
			if pIncident.parameterType == 1 and pIncident.placeType == 1:
				bushes.append(param_id)

		balanceMessage = ''
		
		for param_id in bushes:
			iHandler = incidentHandler
			pBush = parameterIncidents(param_id)
			balanceDate = date.today() - timedelta(days = 1)
			balanceLastCheckDate = iHandler.getIncidentRegisterDate(param_id, 'balance')
			if balanceDate == balanceLastCheckDate:
				print('Пропущеных балансов нет.')
				iHandler.updateIncidentRegisterDate(param_id, balanceDate, 'balance')
			else:
				if not balanceLastCheckDate:
					dates = [balanceDate]
				else:
					dates = stuff.getDatesByDays(balanceLastCheckDate, balanceDate)
				print('По адресу:', pBush.metadata['parentPlaceName'], pBush.metadata['placeName'], 'пропущено', len(dates), 'точек.')
				# и начинаем проверять баланс на каждую из пропущенных дат
				balancePart = ''
				for date in dates:
					pBush.setDate(date)
					balanceAvailable = pBush.getBalanceAvailability()
					if not balanceAvailable:
						balancePart = balancePart + pBush.getBalanceLackMessage()
					else:
						bp = pBush.getBalanceMessage()
						if bp:
							balancePart = balancePart + pBush.getBalanceMessage()
							iHandler.createIncident({'incidentType': 6, 'description': 'Небаланс.', 'self': pBush})
			balanceMessage = balanceMessage + balancePart
		
		reportDate = date.today() - timedelta(days = 1)
		dailyMessage = stuff.getDailyMessage(reportDate)
		if len(balanceMessage) == 0:
			balanceMessage = '<span>Отклонений по балансу за прошедший день не обнаружено.</span>'
		footer = '<br><br><a href="http://pulsarweb.rsks.su:8080">Система мониторинга пульсар</a>'
		message = dailyMessage + balanceMessage + footer
		header = 'Ежедневная сводка мониторинга за ' + str(reportDate)
		stuff.sendEmail(header, message)		