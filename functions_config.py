import os, sys
from configparser import ConfigParser

dirsep = os.path.sep
folder = sys.path[0] + dirsep
configfile = folder + '..' + dirsep + 'pulsarchecker.config'

config = ConfigParser()

def read_config(section):
    config.read(configfile)
    configPart = {}
    if config.has_section(section):
        items = config.items(section)
        for item in items:
            configPart[item[0]] = item[1]
        return configPart
    else:
        raise Exception('CONFIG_PARSER: Section {0} not found in the config file: {1}'.format(section, configfile))

