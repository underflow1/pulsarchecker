import psycopg2
from functions_config import config

class dboperator:
	def __init__(self):
		self.connection = None
		self.cursor = None

	def connect(self):
		db_config = config.read_section('database')
		self.connection =  psycopg2.connect(**db_config)
		if self.connection:
			self.cursor = self.connection.cursor()
		else: 
			raise Exception('Подключение к базе данных не удалось')

	def queryPrepare(self, query, args):
		arguments = {}
		if len(args) == 0 or len(query) == 0:
			raise Exception('Аргументы или текст запроса пусты')
		for arg in args:
			a = type(args[arg]).__name__
			if a == 'date' or a == 'datetime' or a == 'str':
				arguments[arg] = '\'' + str(args[arg]) + '\''
			else: 
				arguments[arg] = str(args[arg])
			if args[arg] == None:
				arguments[arg] = "NULL"				
			find = "$" + str(arg)
			replacewith = arguments[arg]
			query = query.replace(find, replacewith)
		return query

	def fetchAll(self, query):
		self.cursor.execute(query)
		result = self.cursor.fetchall()
		if result:
			if len(result) == 1:
				return result[0]
			return result
		return False

	def executeInsertUpdate(self, query):
		self.cursor.execute(query)
		self.connection.commit()

db = dboperator()

try:
	print('Подключение к базе данных...')
	db.connect()
except Exception as e:
	print(e)
	exit(0)
else:
	print('Подключение установлено успешно.')
