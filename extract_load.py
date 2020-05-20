import urllib
from sqlalchemy import create_engine 
import pandas as pd 
import multiprocessing
from datetime import datetime
import time
import psutil
import gc 
import sys
import pyodbc
import json

class SqlServerTarget:
    
    def __init__(self, server, database, table, schema):
        #TODO: validate inputs and raise exceptions if criteria not met.
        self._server = server
        self._database = database
        self._table = table
        self._schema = schema

        # source specific properties
        self._previous_change_key = None
        self._current_change_key = None
        self._current_source_change_log_id = None

        # destination specific properties
        self._chunk_size = None
        self._table_metadata_logical_id = None

    def create_connection_string(self):
        # 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+'; Trusted_Connection=yes'
        return 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self._server + ';DATABASE=' + self._database + '; Trusted_Connection=yes'

    def get_server_name(self):
        return self._server

    def get_database_name(self):
        return self._database

    def get_table_name(self):
        return self._table

    def get_schema_name(self):
        return self._schema

    def get_chunk_size(self):
        return self._chunk_size

    def set_chunk_size(self, chunk_size):
        self._chunk_size = chunk_size

    def get_previous_change_key(self):
        return self._previous_change_key

    def set_previous_change_key(self, previous_change_key):
        self._previous_change_key = previous_change_key

    def get_current_change_key(self):
        return self._current_change_key

    def set_current_change_key(self, current_change_key):
        self._current_change_key = current_change_key

    def get_current_source_change_log_id(self):
        return self._current_source_change_log_id

    def set_current_source_change_log_id(self, source_change_log_id):
        self._current_source_change_log_id = source_change_log_id

    def get_table_metadata_logical_id(self):
        return self._table_metadata_logical_id

    def set_table_metadata_logical_id(self, table_metadata_logical_id):
        self._table_metadata_logical_id = table_metadata_logical_id

    def create_engine(self):
        connection_string = self.create_connection_string()
        params = urllib.parse.quote_plus(connection_string)
        return create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)

class Consumer(multiprocessing.Process): 
    def __init__(self, task_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue

    def run(self):
        name = self.name
        print(f'{name}: entering')
        while True:
            try:
                
                task = self.task_queue.get()
                
                if task is None:
                    print(f'{name}: Exiting.')
                    self.task_queue.task_done()
                    print(f'{name}: Elapsed time {time.process_time()}')
                    break

                task()
                # clear data to free memory (doesn't work)
                task = None
                self.task_queue.task_done()
            except:
                print(f'{name}: Error occurred. Exiting')
                self.terminate()
            
        return

class Task(object):
    def __init__(self, chunk, server, table, database, schema):
        self.chunk = chunk
        self.table = table
        self.database = database
        self.target = SqlServerTarget(server, database, table, schema)
        
    def __call__(self):
        print(f'Preparing to load chunk {self.chunk[0]}.')
        # load records 
        self.chunk[1].to_sql(name=self.table, index=False, schema=self.target.get_schema_name(), if_exists='append', con=self.target.create_engine())
        print(f'Loaded chunk {self.chunk[0]}.')


def main(source, destination):

    print('Main: Starting.')

    conn = source.create_engine()

    # create a column listing to handle max date limitations in Pandas
    column_list = ''
    columns = get_columns(source)

    for index, column in enumerate(columns):
        # this is required because pandas dataframe has a max date of 4/11/2262 -- MAC 2020-05-18 
        if column[2] == 'datetime':
            column_list += f"CASE WHEN t.{column[1]} > '4/11/2262' THEN NULL ELSE t.{column[1]} END {column[1]}"
        elif column[2] == 'timestamp': 
            column_list += f"CONVERT(VARCHAR(MAX), CONVERT(BINARY(8), {column[1]}), 1) {column[1]}"
        else:
            column_list += f"t.{column[1]}"
        
        if not (len(columns)-1) == index:
            column_list += ', '

    if source.get_previous_change_key() is not None:
        primary_key_list = ''
        primary_key_predicate = ''
        primary_keys = get_primary_keys(source)
        for index, key in enumerate(primary_keys):
            primary_key_list += f'{key[1]}'
            primary_key_predicate += f'ct.{key[1]} = t.{key[1]}'
            if not (len(primary_keys)-1) == index:
                primary_key_list += ','
                primary_key_predicate += ' AND '

        query = f"""SELECT {column_list} FROM {source.get_database_name()}.{source.get_schema_name()}.{source.get_table_name()} t JOIN (SELECT {primary_key_list} FROM CHANGETABLE(CHANGES {source.get_schema_name()}.{source.get_table_name()}, {source.get_previous_change_key()}) ct ) ct ON {primary_key_predicate}"""
    else: 
        query = f"""SELECT {column_list} FROM {source.get_database_name()}.{source.get_schema_name()}.{source.get_table_name()} t"""

    generator = pd.read_sql_query(query, conn, chunksize=(25000 if source.get_chunk_size() is None else source.get_chunk_size()))
    
    tasks = multiprocessing.JoinableQueue()

    num_consumers = multiprocessing.cpu_count()
    print(f'Main: Creating {num_consumers} consumers.')
    consumers = create_consumers(tasks, num_consumers)

    for chunk in enumerate(generator):
        # check that we have a memory buffer before proceeding
        svmem = psutil.virtual_memory()
        print(svmem)
        while svmem.percent > 80:
            print(f'Main: Memory usage greater than 80%.')
            # wait for all tasks to complete
            tasks.join()
            print(f'Main: Tasks in queue completed; Creating new joinable queue.')
            destroy_consumers(consumers)
            # create new joinabe queue to replace the old one
            tasks = multiprocessing.JoinableQueue()
            print(f'Main: New joinable queue created; Running garbage collection.')
            # run garbage collection to free memory
            gc.collect()
            print(f'Main: Garbage collection ran; Checking memory usage again.')
            # recheck memory utilization
            svmem = psutil.virtual_memory()
            consumers = create_consumers(tasks, num_consumers)

        tasks.put(Task(chunk, destination.get_server_name(), f'STG_{source.get_database_name().upper()}_{source.get_table_name().upper()}', destination.get_database_name(), destination.get_schema_name()))

    print(f'Main: Waiting for tasks to complete.')
    tasks.join()

    for _ in range(num_consumers):
        tasks.put(None)

    # execute SHS_SP_PSA_LOAD
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        crsr.execute("EXEC psa.SHS_SP_PSA_LOAD_TABLE @LogicalId = ?", source_target.get_table_metadata_logical_id())
      
    # update source change log set to complete 
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        crsr.execute("EXEC dbo.SHS_SP_UPD_SOURCE_CHANGE_LOG @change_log_id = ?", source_target.get_current_source_change_log_id())

    print(f'Main: Elapsed time {time.process_time()}.')
    print('Main: Leaving.')

def create_consumers(tasks, num_consumers):
    print('create_consumers: Entering.')
    print('create_consumers: num_consumers {0}.'.format(num_consumers))
    
    consumers = [Consumer(tasks) for i in range(num_consumers)]

    for consumer in consumers:
        consumer.start()
        print(f'create_consumers: Start Consumer {consumer.pid}')

    print('create_consumers: Leaving.')
    return consumers

def destroy_consumers(consumers):
    print('destroy_consumers: Entering.')
    for consumer in consumers:
        print(f'destroy_consumers: Terminate Consumer {consumer.pid}')
        consumer.terminate()
        while consumer.is_alive():
            time.sleep(1)
            
        print(f'destroy_consumers: Close Consumer {consumer.pid}')
        consumer.close()

    print('destroy_consumers: Leaving.')

    
def get_columns(source_target):
    with pyodbc.connect(source_target.create_connection_string()) as conn: 
        query = f"""
            select 
                c.column_id,
                c.name column_name,
                t.name data_type,
                c.max_length length,
                c.precision precision,
                c.scale scale 
            from sys.tables ta
                join sys.columns c on c.object_id = ta.object_id 
                join sys.types t on t.system_type_id = c.system_type_id
                    AND t.name <> 'sysname'
            where ta.name = '{source_target.get_table_name()}'
            order by column_id asc
        """
        crsr = conn.cursor()
        crsr.execute(query)
        return crsr.fetchall()

def get_primary_keys(source_target): 
    with pyodbc.connect(source_target.create_connection_string()) as conn:
        query = f"""
            select
                c.column_id column_id,
                c.name column_name,
                t.name data_type,
                c.max_length length,
                c.precision precision,
                c.scale scale
            from sys.indexes i
                join sys.index_columns ic on ic.object_id = i.object_id
                    and ic.index_id = i.index_id
                join sys.columns c on c.object_id = i.object_id
                    and c.column_id = ic.column_id
                join sys.types t on t.system_type_id = c.system_type_id
                    AND t.name <> 'sysname'
            where i.object_id = (
                select
                    object_id
                from sys.tables t
                    join sys.schemas s on s.schema_id = t.schema_id
                        and s.name = '{source_target.get_schema_name()}'
                where t.name = '{source_target.get_table_name()}'
                        )
                and i.is_primary_key = 1
            order by column_id asc
        """
        crsr = conn.cursor()
        crsr.execute(query)
        return crsr.fetchall()

def column_metadata_to_json(primary_keys):
    ret = '['
    for index, row in enumerate(primary_keys): 
        ret += '{ "column_id" : "' + str(row[0]) + '", "column_name" : "' + str(row[1]) + '", "data_type" : "' + str(row[2]) + '", "length" : "' + str(row[3]) + '", "precision" : "' + str(row[4]) + '", "scale" : "' + str(row[5]) + '"}'
        if not (len(primary_keys)-1) == index:
            ret += ','
    ret += ']'
    return ret

def check_change_tracking (source_target):
    check_change_tracking_query = f"SELECT COUNT(1) FROM {source_target.get_database_name()}.sys.change_tracking_tables ctt JOIN {source_target.get_database_name()}.sys.tables t ON t.object_id = ctt.object_id AND t.name = '{source_target.get_table_name()}'"
    with pyodbc.connect(source_target.create_connection_string()) as conn:
        cursor = conn.cursor()
        cursor.execute(check_change_tracking_query)
        return bool(cursor.fetchval())

def add_change_tracking (source_target):
    add_change_tracking_query = f"ALTER TABLE {source_target.get_database_name()}.{source_target.get_schema_name()}.{source_target.get_table_name()} ENABLE CHANGE_TRACKING"
    with pyodbc.connect(source_target.create_connection_string()) as conn:
        cursor = conn.cursor()
        cursor.execute(add_change_tracking_query)

def get_new_change_tracking_key(source_target):
    print('get_new_change_tracking_key: get_new_change_tracking_key Entering.')
    get_new_change_key_query = f"SELECT CHANGE_TRACKING_CURRENT_VERSION()"
    with pyodbc.connect(source_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        ret = crsr.execute(get_new_change_key_query).fetchval()
    print('get_new_change_tracking_key: new change key {0}.'.format(ret))
    print('get_new_change_tracking_key: get_new_change_tracking_key Leaving.')
    return ret

def get_previous_change_tracking_key(source_target, destination_target):
    print('get_previous_change_tracking_key: get_previous_change_tracking_key Entering.')
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        table_name = source_target.get_table_name()
        database_name = source_target.get_database_name()

        print('get_previous_change_tracking_key: table_name {0}.'.format(table_name))
        print('get_previous_change_tracking_key: database_name {0}.'.format(database_name))
    
        params = (table_name, database_name)
        
        crsr = conn.cursor()
        crsr.execute(f"DECLARE @output INT; EXEC {destination_target.get_database_name()}.[dbo].[SHS_SP_SRC_SOURCE_CHANGE_LOG] @table_name = ?, @database_name = ?, @last_change_version = @output OUTPUT; SELECT @output", params)
        ret = crsr.fetchval()

    print('get_previous_change_tracking_key: previous change key {0}.'.format(ret))
    print('get_previous_change_tracking_key: get_previous_change_tracking_key Leaving.')
    return ret

def insert_source_change_log(source_target, destination_target):
    print('insert_source_change_log: insert_source_change_log Entering.')
    
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        table_name = source_target.get_table_name()
        database_name = source_target.get_database_name()
        current_change_key = source_target.get_current_change_key()
        
        print('insert_source_change_log: table name {0}.'.format(table_name))
        print('insert_source_change_log: database name {0}.'.format(database_name))
        print('insert_source_change_log: current change key {0}.'.format(current_change_key))

        params = (table_name, database_name, current_change_key)
        crsr = conn.cursor()
        crsr.execute(f"DECLARE @output INT; EXEC {destination_target.get_database_name()}.[dbo].[SHS_SP_INS_SOURCE_CHANGE_LOG] @table_name = ?, @database_name = ?, @change_version = ?, @change_log_id = @output OUTPUT; SELECT @output", params)
        ret = crsr.fetchval()

    print('insert_source_change_log: source change log id {0}.'.format(ret))
    print('insert_source_change_log: insert_source_change_log Leaving.')
    return ret

def update_source_change_log(source_target, destination_target): 
    pass

def insert_table_metadata_logical_id(source_target, destination_target):
    print('insert_table_metadata_logical_id: insert_table_metadata_logical_id Entering.')
    
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        table_name = source_target.get_table_name()
        source_database_name = source_target.get_database_name()
        target_database_name = destination_target.get_database_name()
        target_schema_name = destination_target.get_schema_name()
        
        print('insert_table_metadata_logical_id: source table name {0}.'.format(table_name))
        print('insert_table_metadata_logical_id: source database name {0}.'.format(source_database_name))
        print('insert_table_metadata_logical_id: target database name {0}.'.format(target_database_name))
        print('insert_table_metadata_logical_id: target scheme name {0}.'.format(target_schema_name))

        params = (table_name, source_database_name, target_database_name, target_schema_name)
        crsr = conn.cursor()
        crsr.execute(f"DECLARE @output INT; EXEC {destination_target.get_database_name()}.[dbo].[SHS_SP_INS_TABLE_METADATA] @source_table_name = ?, @source_database_name = ?, @target_database_name = ?, @target_schema_name = ?, @logical_id = @output OUTPUT; SELECT @output", params)
        ret = crsr.fetchval()

    print('insert_table_metadata_logical_id: table metadata logical id {0}.'.format(ret))
    print('insert_table_metadata_logical_id: insert_source_change_log Leaving.')

    return ret 

def get_table_metadata_logical_id(source_target, destination_target):
    print('get_table_metadata_logical_id: get_table_metadata_logical_id Entering.')
    
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        table_name = source_target.get_table_name()
        database_name = source_target.get_database_name()
        schema_name = source_target.get_schema_name()
        
        print('get_table_metadata_logical_id: table name {0}.'.format(table_name))
        print('get_table_metadata_logical_id: database name {0}.'.format(database_name))
        print('get_table_metadata_logical_id: schema name {0}.'.format(schema_name))

        params = (table_name, database_name, schema_name)
        crsr = conn.cursor()
        crsr.execute(f"DECLARE @output INT; EXEC {destination_target.get_database_name()}.[dbo].[SHS_SP_SRC_TABLE_METADATA] @source_table_name = ?, @source_database_name = ?, @source_schema_name = ?, @logical_id = @output OUTPUT; SELECT @output;", params)
        ret = crsr.fetchval()

    print('get_table_metadata_logical_id: table metadata logical id {0}.'.format(ret))
    print('get_table_metadata_logical_id: get_table_metadata_logical_id Leaving.')
    return ret

def check_stage_table_exists(source_target, destination_target):
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        stg_table_name = f'STG_{source_target.get_database_name().upper()}_{source_target.get_table_name().upper()}'
        stg_exists_query = f"SELECT COUNT(1) FROM {destination_database}.sys.tables t WHERE t.name = '{stg_table_name}'"
        crsr.execute(stg_exists_query)
        return bool(crsr.fetchval())

def check_psa_table_exists(source_target, destination_target):
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        psa_table_name = f'PSA_{source_target.get_database_name().upper()}_{source_target.get_table_name().upper()}'
        psa_exists_query = f"SELECT COUNT(1) FROM {destination_database}.sys.tables t WHERE t.name = '{psa_table_name}'"
        crsr.execute(psa_exists_query)
        return bool(crsr.fetchval())

def check_table_metadata(source_target, destination_target):
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        stg_table_name = f'STG_{source_target.get_database_name().upper()}_{source_target.get_table_name().upper()}'
        psa_table_name = f'PSA_{source_target.get_database_name().upper()}_{source_target.get_table_name().upper()}'
        print('check_table_metadata: Checking table metadata for {0} and {1}.'.format(stg_table_name, psa_table_name))
        crsr.execute("SELECT COUNT(1) FROM dbo.TABLE_METADATA WHERE STG_TABLE_NAME = '{0}' AND PSA_TABLE_NAME = '{1}'".format(stg_table_name, psa_table_name))
        return bool(crsr.fetchval())

def truncate_stage_table(source_target, destination_target):
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        stg_table_name = f'STG_{source_target.get_database_name().upper()}_{source_target.get_table_name().upper()}'
        print('truncate_stage_table: Truncate Stage table {0} created.'.format(stg_table_name))
        crsr.execute("TRUNCATE TABLE stg.{0}".format(stg_table_name))

def check_source_table_exists(source_target, destination_target):
    with pyodbc.connect(source_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        crsr.execute(f"SELECT COUNT(1) FROM {source_target.get_database_name()}.sys.tables t WHERE t.name = '{source_target.get_table_name()}'")
        return bool(crsr.fetchval())


if __name__ == "__main__":

    if len(sys.argv) >= 2: 
        if sys.argv[1] != None:
            configuration_path = sys.argv[1]
    else:
        configuration_path = r"E:\code\extract_load\configuration.json"

    if not configuration_path == None: 
        with open(configuration_path, "rb") as file:
            try: 
                configuration = json.loads(file.read())
            except Exception as ex: 
                print(f'extract_load.main: Failed loading configuration {configuration_path}.')
                print(f'extract_load.main: {ex}')

    # get destination configuration settings
    destination_server = configuration['destination']['server']
    destination_database = configuration['destination']['database']
    destination_schema = configuration['destination']['schema']

    # loop through sources
    for source in configuration['source']:

        source_database = source['database']
        source_server = source['server']
        source_schema = source['schema']
        # source_chunk_size = source["chunk_size"]

        for table in source['tables']:

            source_target = SqlServerTarget(source_server, source_database, table, source_schema)
            destination_target = SqlServerTarget(destination_server, destination_database, None, destination_schema)

            # check that table exists on source 
            if not check_source_table_exists(source_target=source_target, destination_target=destination_target):
                print('__name__: Table {0} does not exist on in the source.'.format(source_target.get_table_name()))
                break

            with pyodbc.connect(destination_target.create_connection_string()) as conn: 
                crsr = conn.cursor() 
                create_table_metadata = False
                # create stage/psa tables if not exists
                if not check_stage_table_exists(source_target=source_target, destination_target=destination_target):
                    print('__name__: Stage table {0} does not exist. Creating...'.format(table))
                    
                    create_table_metadata = True

                    source_table_primary_keys = column_metadata_to_json(get_primary_keys(source_target=source_target))
                    source_table_columns = column_metadata_to_json(get_columns(source_target=source_target))

                    create_stg_table = f"EXEC {destination_database}.[dbo].[SHS_SP_CREATE_STG_TABLE] @table_name = ?, @database_name = ?, @table_primary_keys = ?, @table_columns = ?"
                    create_stg_table_params = (table, source_database, source_table_primary_keys, source_table_columns)
                    
                    crsr.execute(create_stg_table, create_stg_table_params)
                    crsr.commit()

                    # check that stage table was created successfully
                    if not check_stage_table_exists(source_target=source_target, destination_target=destination_target):
                        # new exception, exit code
                        message = f'__name__: Stage table {0} failed to create table.'
                        print(message)
                        sys.exit(message)

                    print('__name__: Stage table {0} created.'.format(table))

                if not check_psa_table_exists(source_target=source_target, destination_target=destination_target):
                    print('__name__: Persistant Stage table {0} does not exist. Creating...'.format(table))

                    create_table_metadata = True
                    
                    source_table_primary_keys = column_metadata_to_json(get_primary_keys(source_target=source_target))
                    source_table_columns = column_metadata_to_json(get_columns(source_target=source_target))

                    create_psa_table = f"EXEC {destination_database}.[dbo].[SHS_SP_CREATE_PSA_TABLE] @table_name = ?, @database_name = ?, @table_primary_keys = ?, @table_columns = ?"
                    create_psa_table_params = (table, source_database, source_table_primary_keys, source_table_columns)
                    
                    crsr.execute(create_psa_table, create_psa_table_params)
                    crsr.commit()
                    
                    # check that stage table was created successfully
                    if not check_psa_table_exists(source_target=source_target, destination_target=destination_target):
                        # new exception, exit code
                        message = f'__name__: Persistant stage table {0} failed to create table.'
                        print(message)
                        sys.exit(message)

                    print('__name__: Persistant Stage table {0} created.'.format(table))

                # truncate stage
                truncate_stage_table(source_target=source_target, destination_target=destination_target)


                if not check_table_metadata(source_target=source_target, destination_target=destination_target):
                    # create new record and return logical id
                    print('__name__: Creating table metadata record for {0}.'.format(table))
                    source_target.set_table_metadata_logical_id(insert_table_metadata_logical_id(source_target=source_target, destination_target=destination_target))
                else: 
                    # get logical id for existing record
                    print('__name__: Get table metadata record for {0}.'.format(table))
                    source_target.set_table_metadata_logical_id(get_table_metadata_logical_id(source_target=source_target, destination_target=destination_target))

                # check that change tracking exists on the current table
                if not check_change_tracking(source_target):
                    print('__name__: Change tracking not enabled on table {0}. Enabling...'.format(table))
                    # enable change tracking on table
                    add_change_tracking(source_target)
                    if not check_change_tracking(source_target):
                        print('__name__: Failed to enable change tacking on table {0}.'.format(table))
                        exit()
                    print('__name__: Change tracking enabled on table {0}.'.format(table))

                # get new change key
                source_target.set_current_change_key(get_new_change_tracking_key(source_target=source_target))

                # add change record to database 
                source_target.set_current_source_change_log_id(insert_source_change_log(source_target=source_target, destination_target=destination_target))

                # get previous change record
                source_target.set_previous_change_key(get_previous_change_tracking_key(source_target=source_target, destination_target=destination_target))

            # begin processing
            main(source_target, destination_target)