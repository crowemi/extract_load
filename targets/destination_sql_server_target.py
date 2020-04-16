
from targets.base_targets import SqlServerTarget

class DestinationSqlServerTarget(SqlServerTarget):
    def __init__(self, configuration):
        super(DestinationSqlServerTarget, self).__init__(configuration)

    def check_destination_table(self, table_name, source_database_name):
        if not table_name == None: 
            check_destination_table_query = f"SELECT COUNT(1) FROM {self._database}.sys.tables t WHERE t.name = 'STG_{source_database_name.upper()}_{table_name.upper()}'"
            cursor = self._connection.cursor()
            cursor.execute(check_destination_table_query)
        else:
            Exception(f"SqlServerTarget.add_Change_tracking: No table name provided.")

    def create_destination_table(self):
        pass