import pyodbc

class SqlServerTarget:
    
    def __init__(self, configuration):
        #TODO: validate inputs and raise exceptions if criteria not met.
        self._server = configuration["server"]
        self._database = configuration["database"]
        connection_string = self.create_connection_string()
        self._connection = pyodbc.connect(connection_string)

    def create_connection_string(self):
        return f"DRIVER={{SQL Server}}; SERVER={self._server}; DATEBASE={self._database}; Trusted_Connection=yes"

    def get_server(self):
        return self._server

    def get_database(self):
        return self._database