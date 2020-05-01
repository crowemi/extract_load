import pandas as pd
import json
from targets.base_targets import SqlServerTarget

class DestinationSqlServerTarget(SqlServerTarget):


    def __init__(self, server, database, schema, load_psa):
        super(DestinationSqlServerTarget, self).__init__(server, database)
        self._server = server
        self._database = database
        self._schema = schema
        self._load_psa = load_psa


    def check_psa_destination_table(self, source_table_name, source_database_name):
        return self.check_destination_table(f"SELECT COUNT(1) FROM {self._database}.sys.tables t WHERE t.name = 'PSA_{source_database_name.upper()}_{source_table_name.upper()}'")


    def check_stg_destination_table(self, source_table_name, source_database_name):
        return self.check_destination_table(f"SELECT COUNT(1) FROM {self._database}.sys.tables t WHERE t.name = '{self.get_stg_destination_table(source_table_name, source_database_name)}'")


    def check_destination_table(self, query):
        ret = False
        with self._connection as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            ret = bool(cursor.fetchval())
        return ret


    def create_destination_table(self, source_table_name, source_database_name):
        if not source_table_name == None and not source_database_name == None: 
            if not self.check_psa_destination_table(source_table_name, source_database_name):
                create_psa_table = f"EXEC {self._database}.[dbo].[SHS_SP_CREATE_PSA_TABLE] @table_name = ?, @source_database_name = ?"
                create_psa_table_params = (source_table_name, source_database_name)
                #TODO: move this to base
                with self._connection as conn:
                    cursor = conn.cursor()
                    cursor.execute(create_psa_table, create_psa_table_params)
                    cursor.commit()

            if not self.check_stg_destination_table(source_table_name, source_database_name):
                create_stg_table_params = (source_table_name, source_database_name)
                #TODO: move this to base
                with self._connection as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"EXEC {self._database}.[dbo].[SHS_SP_CREATE_STG_TABLE] @table_name = ?, @source_database_name = ?", create_stg_table_params)
                    cursor.commit()
        else:
            Exception(f"SqlServerTarget.add_Change_tracking: No table name provided.")
        
        return self.check_psa_destination_table(source_table_name, source_database_name) and self.check_stg_destination_table(source_table_name, source_database_name)

    #TODO: move this to a change tracking target
    def set_change_tracking_key(self, source_table_name, source_database_name, change_version):
        ret = None
        with self._connection as conn: 
            params = (source_table_name, source_database_name, change_version, )
            crsr = conn.cursor()
            crsr.execute(f"DECLARE @output INT; EXEC {self._database}.[dbo].[SHS_SP_INS_SOURCE_CHANGE_LOG] @table_name = ?, @database_name = ?, @change_version = ?, @change_log_id = @output OUTPUT; SELECT @output", params)
            ret = crsr.fetchval()
            
        return ret

    #TODO: move this to a change tracking target
    def get_previous_change_tracking_key(self, source_table_name, source_database_name):
        ret = None
        with self._connection as conn: 
            params = (source_table_name, source_database_name, )
            crsr = conn.cursor()
            crsr.execute(f"DECLARE @output BIGINT; EXEC {self._database}.[dbo].[SHS_SP_SRC_SOURCE_CHANGE_LOG] @table_name = ?, @database_name = ?, @last_change_version = @output OUTPUT; SELECT @output;", params)
            ret = crsr.fetchval()
        return ret 

    def get_stg_destination_table(self, source_table_name, source_database_name):
        return f'STG_{source_database_name.upper()}_{source_table_name.upper()}'

    def load_psa(self):
        with self._connection as conn:
            crsr = conn.cursor()
            crsr.execute(f"")
            ret = crsr.fetchval()
        return ret


    def load_records(self, source_table_name, source_database_name, records):
        while True:
            record = records.get()
            if(record.empty):
                record.task_done()
                break
            
            #TODO: make schema configurable
            record.to_sql(name=self.get_stg_destination_table(source_table_name, source_database_name), index=False, schema='stg', if_exists='append', con=self.create_engine())
            print('Insert records.')

