import pyodbc

class SqlServerTarget:
    
    def __init__(self, server, database):
        #TODO: validate inputs and raise exceptions if criteria not met.
        self._server = server
        self._database = database
        connection_string = self.create_connection_string()
        self._connection = pyodbc.connect(connection_string)

    def create_connection_string(self):
        # 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+'; Trusted_Connection=yes'
        return 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self._server + ';DATABASE=' + self._database + '; Trusted_Connection=yes'

    def get_server_name(self):
        return self._server

    def get_database_name(self):
        return self._database