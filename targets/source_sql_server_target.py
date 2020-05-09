import pyodbc, json
import pandas as pd
import time
import threading
import asyncio
import concurrent.futures

from multiprocessing import Pool, Process, Queue as multi_queue
from queue import Queue
from datetime import datetime

from targets.base_targets import SqlServerTarget

class SourceSqlServerTarget(SqlServerTarget):

    def __init__(self, server, database, schema, table, destination_sql_target):
        super(SourceSqlServerTarget, self).__init__(server, database)
        #TODO: validate inputs and raise exceptions if criteria not met.
        self._schema = schema
        self._table = table
        self._record_keys = Queue()
        self._primary_keys = self._get_primary_keys()

        self._change_records = Queue()
        self._records = multi_queue()
        #TODO: make this configurable
        self._load_semaphore = threading.BoundedSemaphore(4)
        self._process_semaphore = threading.BoundedSemaphore(4)
        self._semaphore = threading.BoundedSemaphore(4)
        self._destination_sql_target = destination_sql_target



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
        conn = self.create_engine()

        #TODO: Add change tracking functionality
        #TODO: Make chunksize configurable
        generator = pd.read_sql_table(self._table, con = conn, chunksize=10000)

        for index, chunk in enumerate(generator):
            self._records.put([index, chunk])

        # poison pill
        self._records.put(None)

        processes = list()
        
        #start multiprocesses
        for index, _ in enumerate(range(3)):
            p = Process(target=self.multi_process_chunks)
            p.start()
            processes.append(p)

        for process in processes:
            process.join()


    def multi_process_chunks(self):
        while True: 
            record = self._records.get()
            if record == None:
                break

            loop = asyncio.get_running_loop()

            try:
                loop.run_until_complete(self.t(record[0], loop, record[1]))
            finally:
                loop.close()
                
    async def t(self, index, loop, chunk):  
        
        try:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                df = await loop.run_in_executor(pool, self.process_chunks_wrapper, index, chunk)

            print(str(f'{index}: Chunk processed.'))
            print(str(f'{index}: Preparing to load data.'))

            with concurrent.futures.ThreadPoolExecutor() as pool:
                await loop.run_in_executor(pool, self.load_data, index, df)

            print(str(f'{index}: Data loaded.'))
        finally:
            self._semaphore.release()
        

    def load_data(self, index, data_frame):
        print(str(f'{index}: Enter load_data.'))
        data_frame.to_sql(name=self._destination_sql_target.get_stg_destination_table(self._table, self._database), index=False, schema='stg', if_exists='append', con=self._destination_sql_target.create_engine())

    def process_chunks_wrapper(self, index, chunk):
        print(str(f'{index}: Enter process_chunks_wrapper.'))
        self._semaphore.acquire()
        print(str(f'{index}: Acquired semephore.'))
        try:
            ret = self.process_chunks(chunk, None)
            # df_queue = multi_queue()
            # p = Process(target=self.process_chunks, args=(chunk, df_queue))
            # p.start()
            # ret = df_queue.get()
            # p.join()
        finally: 
            print(str(f'{index}: Release semephore.'))
            self._semaphore.release()    
            
        return ret
    
    def process_chunks(self, chunk, df_queue):
        df = pd.DataFrame()
        df['CHANGE_DT'] = chunk.apply(lambda row: datetime.now(), axis=1)
        df['METADATA'] = chunk.apply(lambda row: '', axis=1)
        df['RECORD'] = chunk.apply(lambda row: row.to_json(date_format='iso'), axis=1)
        # df_queue.put(df)
        return df

        








