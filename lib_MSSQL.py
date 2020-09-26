
import pyodbc

"""
Connects to an MSSQL Database
"""
class MSSQL:
	def __init__(self,MSSQL_server, MSSQL_database, MSSQL_username, MSSQL_password):
		"""Initialisation
        
        Args:
            MSSQL_server(str): MSSQL server name
            MSSQL_database(str): MSSQL database name
            MSSQL_username(str): MSSQL username
            MSSQL_password(str): MSSQL password
        Returns: MSSQL: instance of this class
        """
		self.server = MSSQL_server
		self.database = MSSQL_database
		self.username = MSSQL_username
		self.password = MSSQL_password
		self.MSSQL_cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+MSSQL_server+';DATABASE='+MSSQL_database+';UID='+MSSQL_username+';PWD='+ MSSQL_password)
		self.MSSQL_cursor = self.MSSQL_cnxn.cursor()
        
	def getConnection(self):
		"""Returns the instance of the MSSQL connection
        
        Returns: MSSQL connection variable
        """
		return self.MSSQL_cnxn
	