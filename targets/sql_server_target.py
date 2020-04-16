import pyodbc 

class SqlServerTarget: 
    
    def __init__(self, configuration):
        #TODO: validate inputs and raise exceptions if criteria no met.
        self._server = configuration["server"]
        self._database = configuration["database"]
        self._schema = configuration["schema"]
        self._tables = configuration["tables"]
        connection_string = self.create_connection_string()
        self._connection = pyodbc.connect(connection_string) 

    def create_connection_string(self):
        return f"DRIVER={{SQL Server}}; SERVER={self._server}; DATEBASE={self._database}; Trusted_Connection=yes"
         
    def get_tables(self):
        return self._tables

    def get_server(self):
        return self._server

    def get_database(self):
        return self._database

    def get_schema(self):
        return self._server

    def check_change_tracking (self, table_name): 
        if not table_name == None:
            check_change_tracking_query = f"SELECT COUNT(1) FROM {self._database}.sys.change_tracking_tables ctt JOIN {self._database}.sys.tables t ON t.object_id = ctt.object_id AND t.name = '{table_name}'"
            cursor = self._connection.cursor()
            cursor.execute(check_change_tracking_query)
            ret = cursor.fetchval()
        else: 
            Exception(f"SqlServerTarget.check_Change_tracking: No table name provided.")
        return bool(ret)

    def add_change_tracking (self, table_name):
        if not table_name == None: 
            add_change_tracking_query = f"ALTER TABLE {self._database}.{self._schema}.{table_name} ENABLE CHANGE_TRACKING"
            cursor = self._connection.cursor()
            cursor.execute(add_change_tracking_query)
        else:
            Exception(f"SqlServerTarget.add_Change_tracking: No table name provided.")
        return self.check_change_tracking(table_name)

    def create_destination_tables(self):
        pass

    def get_change_records(self):
        pass

    def get_records(self):
        pass

    def load_records(self):
        pass

    





