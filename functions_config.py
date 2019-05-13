import os, sys
from configparser import ConfigParser

dirsep = os.path.sep
folder = sys.path[0] + dirsep
configfile = folder + '..' + dirsep + 'pulsarchecker.config'

class configParserWrapper(ConfigParser):
	def __init__(self):
		ConfigParser.__init__(self)
		self.balanceQueryFile = folder + dirsep + 'balance.sql'
		self.incidentsNoticeTemplate = folder +'email.html'
		self.balanceNoticeTemplate = folder +'balance.html'
		self.dailyReportNoticeTemplate = folder +'dailyreport.html'
		self.averagedays = 7
		self.averagehours = 2
		self.pollinterval = 2
		self.read(configfile)

	def read_section(self, section):
		configPart = {}
		if self.has_section(section):
			items = self.items(section)
			for item in items:
				configPart[item[0]] = item[1]
			return configPart
		else:
			raise Exception('CONFIG_PARSER: Section {0} not found in the config file: {1}'.format(section, configfile))

config = configParserWrapper()
