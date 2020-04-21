import pyodbc 
from targets.base_targets import SqlServerTarget

class SourceSqlServerTarget(SqlServerTarget): 
    
    def __init__(self, configuration):
        super(SourceSqlServerTarget, self).__init__(configuration)
        #TODO: validate inputs and raise exceptions if criteria not met.
        self._schema = configuration["schema"]
        self._tables = configuration["tables"]
         
    def get_tables(self):
        return self._tables

    def check_change_tracking (self, table_name): 
        if not table_name == None:
            check_change_tracking_query = f"SELECT COUNT(1) FROM {self._database}.sys.change_tracking_tables ctt JOIN {self._database}.sys.tables t ON t.object_id = ctt.object_id AND t.name = '{table_name}'"
            with self._connection as conn:
                cursor = conn.cursor()
                cursor.execute(check_change_tracking_query)
                ret = bool(cursor.fetchval())
        else: 
            Exception(f"SqlServerTarget.check_Change_tracking: No table name provided.")
        return ret

    def add_change_tracking (self, table_name):
        if not table_name == None: 
            add_change_tracking_query = f"ALTER TABLE {self._database}.{self._schema}.{table_name} ENABLE CHANGE_TRACKING"
            with self._connection as conn:
                cursor = conn.cursor()
                cursor.execute(add_change_tracking_query)
        else:
            Exception(f"SqlServerTarget.add_Change_tracking: No table name provided.")
        return self.check_change_tracking(table_name)

    def get_new_change_key(self):
        ret = None
        get_new_change_key_query = f"SELECT CHANGE_TRACKING_VERSION()"
        with self._connection as conn:
            crsr = conn.cursor()
            crsr.execute(get_new_change_key_query)
            ret = crsr.fetchval()
        return ret

    def get_change_records(self):
        # this should be records that fall between last change key and current change key 
        pass

    def get_records(self):
        pass



    





