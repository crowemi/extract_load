
from targets.base_targets import SqlServerTarget

class DestinationSqlServerTarget(SqlServerTarget):
    def __init__(self, configuration):
        super(DestinationSqlServerTarget, self).__init__(configuration)

    def check_psa_destination_table(self, table_name, source_database_name):
        if not table_name == None: 
            ret = self.check_destination_table(f"SELECT COUNT(1) FROM {self._database}.sys.tables t WHERE t.name = 'PSA_{source_database_name.upper()}_{table_name.upper()}'")
        else:
            Exception(f"SqlServerTarget.add_Change_tracking: No table name provided.")
        return ret

    def check_stg_destination_table(self, table_name, source_database_name):
        if not table_name == None: 
            ret = self.check_destination_table(f"SELECT COUNT(1) FROM {self._database}.sys.tables t WHERE t.name = 'STG_{source_database_name.upper()}_{table_name.upper()}'")
        else:
            Exception(f"SqlServerTarget.add_Change_tracking: No table name provided.")
        return ret

    def check_destination_table(self, query):
        cursor = self._connection.cursor()
        cursor.execute(query)
        return bool(cursor.fetchval())

    def create_destination_table(self, table_name, source_database_name):
        if not table_name == None: 
            if not self.check_psa_destination_table(table_name, source_database_name):
                cursor = self._connection.cursor()
                cursor.execute("")
            if not self.check_stg_destination_table(table_name, source_database_name):
                pass
        else:
            Exception(f"SqlServerTarget.add_Change_tracking: No table name provided.")
        
        

    def load_records(self):
        pass