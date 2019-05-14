from parameterIncidents import parameterIncidents
from handlerIncidents import incidentHandler
from datetime import datetime, timedelta
import functions_stuff as stuff
import os, sys, time

savedIncidentCounter = 0
autoclosedIncidentCounter = 0
savedincidents = []

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
				iHandler.createIncident(pIncident.getCurrentIncident())
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
				print('Инцидент не выход на связь закрыт.')

		# далее мы строим интервал пропущенных проверок для данного параметра 
		newestDate = pIncident.newestArchiveTime
		lastCheckDate = iHandler.getIncidentRegisterDate(param_id, 'incident')
		if not lastCheckDate:
			lastCheckDate = newestDate
		if (newestDate == lastCheckDate):
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
						print('Создано')
						time.sleep(1)
					else: # а если есть не завершенный открытый инцидент такого же типа, то это он и есть (т.е. инцидент продолжается) и мы смотрим следущую дату
						print('- продолжение старого инцидента', end = ' \r ')
						continue
			iHandler.updateIncidentRegisterDate(param_id, date, 'incident')
		time.sleep(0.1)
		print()
