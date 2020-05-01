import pyodbc, json 
import pandas as pd

import threading

from queue import Queue
from datetime import datetime

from targets.base_targets import SqlServerTarget

class SourceSqlServerTarget(SqlServerTarget): 
    
    def __init__(self, server, database, schema, table):
        super(SourceSqlServerTarget, self).__init__(server, database)
        #TODO: validate inputs and raise exceptions if criteria not met.
        self._schema = schema
        self._table = table
        self._record_keys = Queue()
        self._primary_keys = self._get_primary_keys()

        self._change_records = Queue()
        self._records = Queue()
        #TODO: make this configurable
        self._semaphore = threading.BoundedSemaphore(4) 



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


    def get_change_records(self, previous_change_key, new_change_key):
        if previous_change_key == 0:
            # the extract has never run before, not change key exists in change store
            query = f"select 'I', {self.format_select_primary_keys()} from {self._database}.{self._schema}.{self._table}"
        else:
            query = f"select sys_change_operation, {self.format_select_primary_keys()} from changetable(changes {self._table}, {new_change_key}) ct" 
            
        with self.create_connection() as conn:
            crsr = conn.cursor()
            rows = crsr.execute(query)
            for row in rows:
                self._change_records.put(row)
        # add poison pill for downstream process
        self._change_records.put(None)

    def format_select_primary_keys(self):
        ret = ''
        for index, key in enumerate(self._primary_keys):
            ret += f'{key[0]}'
            if (index + 1) < len(self._primary_keys): 
                ret += ', '
        return ret

    def format_where_primary_keys(self, record):
        ret = ''
        for index, key in enumerate(self._primary_keys):
            ret += f"{key[0]} = '{record[key[1]]}'"
            if (index + 1) < len(self._primary_keys): 
                ret += ' and '
        return ret

    def get_records(self):
        with self.create_connection() as conn:
            #TODO: Add change tracking functionality
            #TODO: Make chunksize configurable 
            query = f"select * from {self._database}.{self._schema}.{self._table}"
            df_chunk = pd.read_sql_query(query, conn, chunksize=5000)

            threads = list()

            for chunk in df_chunk:
                thread = threading.Thread(target=self.process_chunks, args=([chunk]))
                thread.start()
                threads.append(thread)

            for thread in enumerate(threads):
                thread.join()

            # Add poison pill for downstream processes
            self._records.put(pd.DataFrame())

    def process_chunks(self, chunk):
        # print(str(f'{threading.get_ident()} : Awaiting semaphore.'))
        # self._semaphore.acquire()
        # try:
        df = pd.DataFrame()
        df['CHANGE_DT'] = chunk.apply(lambda row: datetime.now(), axis=1)
        df['METADATA'] = chunk.apply(lambda row: '', axis=1)
        df['RECORD'] = chunk.apply(lambda row: row.to_json(date_format='iso'), axis=1)                
        self._records.put(df)
        print(str(f'{threading.get_ident()} : Added records to queue.'))
        # finally:
        #     self._semaphore.release()
        #     print(str(f'{threading.get_ident()} : Released semaphore.'))

        
        

    





