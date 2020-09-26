import pandas as pd
import os
from lib_MSSQL import MSSQL
import datetime
import time
"""
Saves MSSQL data to CSV or Excel
"""
class MSSQL_Saver:
	TAG = 'MSSQL_Saver: '
	def __init__(self,server = None, database = None, username = None, password = None, connection = None):
		"""Initialisation
        
        Args:
            server(str): MSSQL server name
            database(str): MSSQL database name
            username(str): MSSQL username
            password(str): MSSQL password
            connection(str): MSSQL connection
        Returns: MSSQL_Saver: instance of this class
        """
		print(self.TAG, 'instantiate')
		if (server is not None and database is not None and username is not None and password is not None and connection is None):
			self.MSSQL_server = server
			self.MSSQL_database = database
			self.MSSQL_username = username
			self.MSSQL_password = password
			self.mssql_instance = MSSQL(self.MSSQL_server, self.MSSQL_database, self.MSSQL_username, self.MSSQL_password)
			self.mssql_conn = self.mssql_instance.getConnection()
			print(self.TAG, 'connected')
		
		if (connection is None):
			return
		else:
			self.mssql_conn = connection
			print(self.TAG, 'connected, ', connection)
		
	def save(self,name = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")+ '_MSSQL', destination = os.getcwd(), commands = {}, parameters = {}, format = 'CSV', parameterOpenChar = '{', parameterCloseChar = '}', verboseDebug = False):
		"""saves SQL data to CSV or Excel
        
        Args:
            name(str, optional): Name to save
            destination(str, optional): Destination folder
            commands(dict(str, str)): List of MSSQL commands to execute
            parameters(dict(str, str)), optional): parameters to feed commands
            format(str, optional): CSV or Excel
            parameterOpenChar(str, optional): opening character of parameter
            parameterCloseChar(str, optional): closing character of parameter
            verboseDebug(str, optional): Verbose debugging output in console
        Returns: str: destination name
        """
		if(len(commands)==0):
			return 'No commands'
			
		startTime = time.time()
		fullName = os.path.join(destination, name)
		if format == 'CSV':
			os.makedirs(fullName)
		elif format == 'Excel':
			if (not str.endswith('.xlsx')):
				fullName = fullName + '.xlsx'
			excelWriter = pd.ExcelWriter(fullName, engine='xlsxwriter')
		
		print(self.TAG, 'save to: ', fullName)
		for key, command in commands.items():
			inStartTime = time.time()
			print(self.TAG, 'executing: '+ key)
			df = self.executeInstant(command, parameters, parameterOpenChar, parameterCloseChar, verboseDebug = verboseDebug)
			inEndTime = time.time()
			inDiff = inEndTime - inStartTime
			print(self.TAG, 'executed '+ key + ' in ' + str(int(inDiff/60)) + ' minutes and ' + str(int(inDiff%60)) + ' seconds')
			if format == 'CSV':
				df.to_csv(os.path.join(fullName, key+'.csv'), index=False)
			elif format == 'Excel':
				df.to_excel(excelWriter, sheet_name=key, index=False)
		if format == 'Excel':
			excelWriter.save()
		endTime = time.time()
		diff = endTime - startTime
		print(self.TAG, 'All tables saved to ' + fullName + '\nTime taken: ' + str(int(diff/60)) + ' minutes and ' + str(int(diff%60)) + ' seconds')
		return fullName
	@staticmethod
	def __formatString__(string = '', parameters = {}, openChar = '{', closeChar = '}'):
		"""Recursively replace parameter names with parameters themselves
        
        Args:
            string(str): string to operate on
            parameters(dict(str, str)): parameter names and values
            openChar(str, optional): opening character of parameter
            closeChar(str, optional): closing character of parameter
        Returns: str: formatted string
        """
		if(openChar in string and closeChar in string):
			for key, value in parameters.items():
				string = string.replace(openChar + key + closeChar, value)
			return MSSQL_Saver.__formatString__(string, parameters, openChar, closeChar)
		else:
			return string
		
	def executeInstant(self, command, parameters = {}, parameterOpenChar = '{', parameterCloseChar = '}', verboseDebug = False):
		"""Instantly execute a command
        
        Args:
            commands(str): command to execute
            parameters(dict(str, str)), optional): parameters to feed command
            parameterOpenChar(str, optional): opening character of parameter
            parameterCloseChar(str, optional): closing character of parameter
            verboseDebug(str, optional): Verbose debugging output in console
        Returns: dataframe: pandas dataframe containing command output
        """
		formattedCommand = MSSQL_Saver.__formatString__(string = command, parameters = parameters, openChar = parameterOpenChar, closeChar = parameterCloseChar)
		if(verboseDebug):
			print(self.TAG, 'executeInstant\n'+ formattedCommand)
		SQL_Query = pd.read_sql_query(formattedCommand,self.mssql_conn)
		df = pd.DataFrame(SQL_Query)
		if(verboseDebug):
			print(df)
		return df