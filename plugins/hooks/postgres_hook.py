import re
import psycopg2

from psycopg2.extras import execute_values


# Hook for connecting outside the airflow. This class is for connecting to the local postgres
class PostgresHook():
	def __init__(self, connection):
		self.cursor = None
		self.connection = psycopg2.connect(
			f"dbname='postgres' host={connection.host} user={connection.login} port={connection.port} "
			f"password={connection.password}"
		)

	def insert_records(self, query, obj_list):
		# Basic transaction management
		try:
			self.cursor = self.connection.cursor()
			self.cursor.execute("begin transaction;")
			execute_values(self.cursor, query, obj_list)
			self.cursor.execute("end transaction;")
			self.cursor.close()
			self.connection.commit()
		except Exception as e:
			print(e)
			self.cursor.execute("ROLLBACK;")
			self.cursor.close()
			self.connection.commit()

	def update_delete_query(self, query):

		try:
			self.cursor = self.connection.cursor()
			self.cursor.execute("begin transaction;")
			self.cursor.execute(query)
			self.cursor.execute("end transaction;")
			self.cursor.close()
			self.connection.commit()

		except Exception as e:
			print(e)
			self.cursor.execute("ROLLBACK;")
			self.cursor.close()
			self.connection.commit()

	def select_query(self, query):
		self.cursor = self.connection.cursor()
		self.cursor.execute("begin transaction;")
		self.cursor.execute(query)
		db_data = self.cursor.fetchall()
		columns = [desc[0] for desc in self.cursor.description]
		db_obj = list()
		for row in db_data:
			temp = dict()
			for i in range(0, len(row)):
				if re.search("decimal", str(type(row[i]))):
					temp[columns[i]] = float(row[i])
				elif re.search("datetime", str(type(row[i]))):
					temp[columns[i]] = str(row[i])
				else:
					temp[columns[i]] = row[i]
			db_obj.append(temp)

		self.cursor.execute("end transaction;")
		self.cursor.close()
		self.connection.commit()
		return db_obj
