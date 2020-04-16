import pyodbc 

class SqlServerTarget: 
    
    def __init__(self, configuration):
        self._server = configuration.server
        self._database = configuration.database
        self._schema = configuration.schema
        self._tables = configuration.tables
        self._source_connection = pyodbc.connect(self.create_connection_string()) 

    def create_connection_string(self):
        return "DRIVER={SQL Server}; SERVER=" + self._server + "; DATEBASE=" + self._database + "; Trusted_Connection=yes",
         
    def get_tables(self):
        return self._tables

    # check if table has change tracking configured
    def check_change_tracking (self): 
        pass

    def add_change_tracking (self):
        pass

    def create_destination_tables(self):
        pass

    def get_records(self):
        pass

    def load_records(self):
        pass

    





