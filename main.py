from parameterIncidents import parameterIncidents
from handlerIncidents import incidentHandler
from datetime import datetime, timedelta, date, time
import functions_stuff as stuff

savedIncidentCounter = 0
autoclosedIncidentCounter = 0
savedincidents = []

if len(sys.argv) == 1:
	parametersList = stuff.getParamCheckList()
	for param_id in parametersList:
		iHandler = incidentHandler()
		pIncident = parameterIncidents(param_id)
		activeLostIncident = False
		activeOtherIncident = False
		activeOtherIncidentTime = False
		activeLostIncidentTime = False

		# Проверка наличия активного инцидента connection lost
		connectionLostIncident = iHandler.getExistingIncident(param_id, 1)
		# Проверка связи
		connectionActive = not pIncident(connectionLostIncident).connectionActive

		if not (connectionActive and connectionLostIncident):
			# если активного инцидента нет, то его нужно создать
			incident = pIncident.getCurrentIncident()
			if incident:
				iHandler.createIncident(incident)
				iHandler.updateIncidentRegisterDate(param_id, incident['self'].date, 'incident')
		else:

