
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
        ret = False
        with self._connection as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            ret = bool(cursor.fetchval())
        return ret

    def create_destination_table(self, table_name, source_database_name):
        if not table_name == None and not source_database_name == None: 
            if not self.check_psa_destination_table(table_name, source_database_name):
                create_psa_table = f"EXEC {self._database}.[dbo].[SHS_SP_CREATE_PSA_TABLE] @table_name = ?, @source_database_name = ?"
                create_psa_table_params = (table_name, source_database_name)
                #TODO: move this to base
                with self._connection as conn:
                    cursor = conn.cursor()
                    cursor.execute(create_psa_table, create_psa_table_params)
                    cursor.commit()

            if not self.check_stg_destination_table(table_name, source_database_name):
                create_stg_table = f"EXEC {self._database}.[dbo].[SHS_SP_CREATE_STG_TABLE] @table_name = ?, @source_database_name = ?"
                create_stg_table_params = (table_name, source_database_name)
                #TODO: move this to base
                with self._connection as conn:
                    cursor = conn.cursor()
                    cursor.execute(create_stg_table, create_stg_table_params)
                    cursor.commit()
        else:
            Exception(f"SqlServerTarget.add_Change_tracking: No table name provided.")
        
        return self.check_psa_destination_table(table_name, source_database_name) and self.check_stg_destination_table(table_name, source_database_name)
         

    def load_records(self):
        pass