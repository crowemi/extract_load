import pyodbc 
from queue import Queue

from targets.base_targets import SqlServerTarget

class SourceSqlServerTarget(SqlServerTarget): 
    
    def __init__(self, server, database, schema, table):
        super(SourceSqlServerTarget, self).__init__(server, database)
        #TODO: validate inputs and raise exceptions if criteria not met.
        self._schema = schema
        self._table = table
        self._record_keys = Queue()
        self._primary_keys = self._get_primary_keys()


    def get_table_name(self):
        return self._table


    def check_change_tracking (self): 
        check_change_tracking_query = f"SELECT COUNT(1) FROM {self._database}.sys.change_tracking_tables ctt JOIN {self._database}.sys.tables t ON t.object_id = ctt.object_id AND t.name = '{self._table}'"
        with self._connection as conn:
            cursor = conn.cursor()
            cursor.execute(check_change_tracking_query)
            return bool(cursor.fetchval())


    def add_change_tracking (self):
        add_change_tracking_query = f"ALTER TABLE {self._database}.{self._schema}.{self._table} ENABLE CHANGE_TRACKING"
        with self._connection as conn:
            cursor = conn.cursor()
            cursor.execute(add_change_tracking_query)
        return self.check_change_tracking()


    def get_new_change_tracking_key(self):
        ret = None
        get_new_change_key_query = f"SELECT CHANGE_TRACKING_CURRENT_VERSION()"
        with self._connection as conn:
            crsr = conn.cursor()
            ret = crsr.execute(get_new_change_key_query).fetchval()
        return ret


    def _get_primary_keys(self):
        primary_keys = []
        with self._connection as conn:
            query = f"""
                select 
                    c.name,
                    ic.key_ordinal 
                from sys.indexes i
                    join sys.index_columns ic on ic.object_id = i.object_id
                        and ic.index_id = i.index_id
                    join sys.columns c on c.object_id = i.object_id
                        and c.column_id = ic.column_id
                where i.object_id = (
                    select
                        object_id
                    from sys.tables
                    where name = '{self._table}'
                ) 
                    AND i.is_primary_key = 1           
            """
            crsr = conn.cursor()
            crsr.execute(query)
            rows = crsr.fetchall()
            for row in rows:
                primary_keys.append((row.name, row.key_ordinal))
        return primary_keys


    def get_change_records(self):
        # this should be records that fall between last change key and current change key 
        pass


    def get_records(self):
        pass



    





