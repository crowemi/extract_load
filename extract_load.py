import urllib
from sqlalchemy import create_engine 
import pandas as pd 
import multiprocessing
from datetime import datetime
import time
import psutil
import gc 
import sys, traceback
import pyodbc
import json
import os, glob
import binascii

# targets 
from sources.extract_sql_server import SqlServerTarget
from sources.extract_excel import ExcelTarget
from sources.extract_csv import CsvTarget

import extract_log

import logging

class Consumer(multiprocessing.Process): 
    def __init__(self, task_queue, log_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.log_queue = log_queue

    def run(self):
        name = self.name

        extract_log.worker_configurer(self.log_queue)
        logger = logging.getLogger()
        logger.log(logging.INFO, f'{name}: entering')
        
        while True:
            try:
                
                task = self.task_queue.get()
                
                if task is None:
                    logger.info(f'{name}: Exiting.')
                    self.task_queue.task_done()
                    logger.info(f'{name}: Elapsed time {time.process_time()}')
                    break

                try:
                    task(logger)
                except:
                    logger.error(f'Unexpexted error: {sys.exc_info()[0]}')
                    raise Exception()
                
                # clear data to free memory (doesn't work)
                task = None
                self.task_queue.task_done()
            except:
                logger.info(f'{name}: Error occurred. Exiting')
                self.terminate()
            
        return

class Task(object):
    def __init__(self, chunk, server, table, database, schema, is_chunks, psa_only):
        self.chunk = chunk
        self.table = table
        self.database = database
        self.target = SqlServerTarget(server, database, table, schema)
        self.is_chunks = is_chunks
        self.logger = logging.getLogger()
        self.psa_only = psa_only

        # look up the current
        if self.psa_only: 
            # get current chunk records for lookup
            chunk[1]['RECORD_UID'].to_sql(
                name=f'{table}_CHUNK_{chunk[0]}', #temp table name for this chunk
                schema='tmp', 
                index=False, 
                if_exists='replace', #drop table it the temp table already exists
                con=self.target.create_engine()
            )
                
            query=f"SELECT psa.HPXR_UID, psa.RECORD_UID, psa.RECORD_HASH FROM {database}.{schema}.{table} psa JOIN tmp.{table}_CHUNK_{chunk[0]} tmp ON tmp.RECORD_UID = CONVERT(VARCHAR(MAX), psa.RECORD_UID) WHERE psa.IS_CURRENT = 1"

            lookup = pd.read_sql_query(
                sql=query,
                con=self.target.create_engine(isolation_level="SNAPSHOT")
            )
            merge_df = pd.merge(chunk[1], lookup[['RECORD_UID', 'RECORD_HASH']], how='left', indicator=True, on=['RECORD_UID', 'RECORD_HASH'])
            
            self.load_df = merge_df[merge_df['_merge'] == 'left_only']
            self.load_df = self.load_df.drop(['_merge'], axis=1)

            # get another subset merged on 
            self.update_df = pd.merge(lookup, self.load_df[['RECORD_UID']], how='inner', on=['RECORD_UID'])
        else: 
            self.load_df = chunk[1]
            self.update_df = pd.DataFrame()

    def process_record_chunk(self):
        self.logger.info(f'Preparing to load chunk {self.chunk[0]}.')
        if not self.load_df.empty:
            try:
                # load records 
                self.load_df.to_sql(name=self.table, index=False, schema=self.target.get_schema_name(), if_exists='append', con=self.target.create_engine())
                self.logger.info(f'Loaded chunk {self.chunk[0]}.')
                
                if len(self.update_df) > 0 and self.psa_only: 
                    for row in self.update_df.itertuples():
                        try:
                            with pyodbc.connect(self.target.create_connection_string()) as conn:
                                crsr = conn.cursor()
                                update_is_current = "UPDATE psa SET IS_CURRENT = 0 FROM {0}.{1}.{2} psa WHERE psa.HPXR_UID = 0x{3}".format(
                                        self.target.get_database_name(), 
                                        self.target.get_schema_name(), 
                                        self.target.get_table_name(), 
                                        binascii.b2a_hex(row.HPXR_UID).decode('utf-8') #convert to hexidecimal to join up with SQL Server 
                                    )
                                crsr.execute(update_is_current)
                                crsr.close()

                        except pyodbc.Error as e: 
                            logger.error(f'{e}')
                            raise Exception(e)
                        except Exception as e: 
                            logger.error(f'Unexpexted error: {e}')
                            raise Exception(e)
                
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.logger.error(f"Unable to load chunk {self.chunk[1]}")
                self.logger.error(f'Unexpexted error: {traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)}')
                raise sys.exit()
        try:
            with pyodbc.connect(self.target.create_connection_string()) as conn:
                crsr = conn.cursor()
                drop_temp_table = f"DROP TABLE IF EXISTS tmp.{self.table}_CHUNK_{self.chunk[0]}"
                crsr.execute(drop_temp_table)
                crsr.close()
        except: 
            self.logger.error(f"Unable to drop table tmp.{self.table}_CHUNK_{self.chunk[0]}")
            self.logger.error(f'Unexpexted error: {sys.exc_info()[0]}')
            raise Exception()

    def process_record(self):
        self.logger.info(f'Preparing to load chunk {self.chunk}.')
        try:
            # load records 
            self.chunk.to_sql(name=self.table, index=False, schema=self.target.get_schema_name(), if_exists='append', con=self.target.create_engine())
            self.logger.info(f'Loaded chunk {self.chunk}.')
        except:
            self.logger.error(f"Unable to load chunk {self.chunk}")
            self.logger.error(f'Unexpexted error: {sys.exc_info()[0]}')
            raise Exception()

    def __call__(self, logger):
        if self.is_chunks:
            self.process_record_chunk()
        else:
            self.process_record()


def extract_load(generator, log_queue, destination_target, is_chunks, psa_only=None):

    logger = logging.getLogger()
    logger.info('Main: Starting.')
    
    tasks = multiprocessing.JoinableQueue()

    num_consumers = multiprocessing.cpu_count()
    logger.info(f'Main: Creating {num_consumers} consumers.')
    consumers = create_consumers(tasks, num_consumers, log_queue)

    if is_chunks:
        for chunk in enumerate(generator):
            # check that we have a memory buffer before proceeding
            svmem = psutil.virtual_memory()
            logger.debug(svmem)
            while svmem.percent > 80:
                logger.warning(svmem)
                logger.warning(f'Main: Memory usage greater than 80%.')
                # wait for all tasks to complete
                tasks.join()
                logger.warning(f'Main: Tasks in queue completed; Creating new joinable queue.')
                destroy_consumers(consumers)
                # create new joinabe queue to replace the old one
                tasks = multiprocessing.JoinableQueue()
                logger.warning(f'Main: New joinable queue created; Running garbage collection.')
                # run garbage collection to free memory
                gc.collect()
                logger.warning(f'Main: Garbage collection ran; Checking memory usage again.')
                # recheck memory utilization
                svmem = psutil.virtual_memory()
                consumers = create_consumers(tasks, num_consumers, log_queue)

            tasks.put(
                Task(
                    chunk, 
                    destination_target.get_server_name(), 
                    destination_target.get_table_name(), 
                    destination_target.get_database_name(), 
                    destination_target.get_schema_name(),
                    is_chunks,
                    psa_only
                )
            )
    else:
        tasks.put(
            Task(
                generator, 
                destination_target.get_server_name(), 
                destination_target.get_table_name(), 
                destination_target.get_database_name(), 
                destination_target.get_schema_name(),
                is_chunks,
                psa_only
            )
        )

    logger.info(f'Main: Waiting for tasks to complete.')
    tasks.join()

    for _ in range(num_consumers):
        tasks.put(None)

    logger.info(f'Main: Elapsed time {time.process_time()}.')
    logger.info('Main: Leaving.') 

    # destroy consumers before proceeding
    destroy_consumers(consumers)

def create_consumers(tasks, num_consumers, log_queue):
    logger = logging.getLogger() #root logger
    logger.info('create_consumers: Entering.')
    logger.info('create_consumers: num_consumers {0}.'.format(num_consumers))
    
    consumers = [Consumer(tasks, log_queue) for i in range(num_consumers)]

    for consumer in consumers:
        consumer.start()
        logger.info(f'create_consumers: Start Consumer {consumer.pid}')

    logger.info('create_consumers: Leaving.')
    return consumers

def destroy_consumers(consumers):

    logging.info('destroy_consumers: Entering.')
    for consumer in consumers:
        logging.info(f'destroy_consumers: Terminate Consumer {consumer.pid}')
        consumer.terminate()
        while consumer.is_alive():
            time.sleep(1)
            
        logging.info(f'destroy_consumers: Close Consumer {consumer.pid}')
        consumer.close()

    logging.info('destroy_consumers: Leaving.')


#TODO: Create unit test
def column_metadata_to_json(columns):
    """Converts column metadata (column_id, column_name, data_type, length, precision, scale) to json string. 

        Keyword arguments:
        columns -- list of columns
    """

    ret = '['
    for index, row in enumerate(columns): 
        ret += '{ "column_id" : "' + str(row[0]) + '", "column_name" : "' + str(row[1]) + '", "data_type" : "' + str(row[2]) + '", "length" : "' + str(row[3]) + '", "precision" : "' + str(row[4]) + '", "scale" : "' + str(row[5]) + '"}'
        if not (len(columns)-1) == index:
            ret += ','
    ret += ']'
    return ret

#TODO: Create unit test
def get_previous_change_tracking_key(source_target, destination_target):
    logging.debug('get_previous_change_tracking_key: get_previous_change_tracking_key Entering.')
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        table_name = source_target.get_table_name()
        database_name = source_target.get_database_name()

        logging.info('get_previous_change_tracking_key: table_name {0}.'.format(table_name))
        logging.info('get_previous_change_tracking_key: database_name {0}.'.format(database_name))
    
        params = (table_name, database_name)
        
        crsr = conn.cursor()
        crsr.execute(f"DECLARE @output INT; EXEC {destination_target.get_database_name()}.[dbo].[SHS_SP_SRC_SOURCE_CHANGE_LOG] @table_name = ?, @database_name = ?, @last_change_version = @output OUTPUT; SELECT @output", params)
        ret = crsr.fetchval()

    logging.info('get_previous_change_tracking_key: previous change key {0}.'.format(ret))
    logging.debug('get_previous_change_tracking_key: get_previous_change_tracking_key Leaving.')
    return ret

#TODO: Create unit test
def insert_source_change_log(source_target, destination_target):
    logging.debug('insert_source_change_log: insert_source_change_log Entering.')
    
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        table_name = source_target.get_table_name()
        database_name = source_target.get_database_name()
        current_change_key = source_target.get_current_change_key()
        
        logging.info('insert_source_change_log: table name {0}.'.format(table_name))
        logging.info('insert_source_change_log: database name {0}.'.format(database_name))
        logging.info('insert_source_change_log: current change key {0}.'.format(current_change_key))

        params = (table_name, database_name, current_change_key)
        crsr = conn.cursor()
        crsr.execute(f"DECLARE @output INT; EXEC {destination_target.get_database_name()}.[dbo].[SHS_SP_INS_SOURCE_CHANGE_LOG] @table_name = ?, @database_name = ?, @change_version = ?, @change_log_id = @output OUTPUT; SELECT @output", params)
        ret = crsr.fetchval()

    logging.info('insert_source_change_log: source change log id {0}.'.format(ret))
    logging.debug('insert_source_change_log: insert_source_change_log Leaving.')
    return ret

#TODO: Create unit test
def update_source_change_log(source_target, destination_target): 
    pass

def insert_table_metadata_logical_id(process_name, vendor_name, destination_target):
    """Insert a new record into the dbo.TABLE_METADATA to configure a new record for this source.

        Keyword arguments:
        source_target - 
        destination_target -    

    """

    logging.debug('insert_table_metadata_logical_id: insert_table_metadata_logical_id Entering.')
    
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        table_name = process_name
        source_database_name = vendor_name
        target_database_name = destination_target.get_database_name()
        target_schema_name = destination_target.get_schema_name()
        
        logging.debug('insert_table_metadata_logical_id: source table name {0}.'.format(table_name))
        logging.debug('insert_table_metadata_logical_id: source database name {0}.'.format(source_database_name))
        logging.debug('insert_table_metadata_logical_id: target database name {0}.'.format(target_database_name))
        logging.debug('insert_table_metadata_logical_id: target scheme name {0}.'.format(target_schema_name))

        params = (table_name, source_database_name, target_database_name, target_schema_name)
        crsr = conn.cursor()
        crsr.execute(f"DECLARE @output INT; EXEC {destination_target.get_database_name()}.[dbo].[SHS_SP_INS_TABLE_METADATA] @source_table_name = ?, @source_database_name = ?, @target_database_name = ?, @target_schema_name = ?, @logical_id = @output OUTPUT; SELECT @output", params)
        ret = crsr.fetchval()

    logging.info('insert_table_metadata_logical_id: table metadata logical id {0}.'.format(ret))
    logging.debug('insert_table_metadata_logical_id: insert_source_change_log Leaving.')

    return ret 

#TODO: Create unit test
def get_table_metadata_logical_id(process_name, vendor_name, destination_target):
    logging.info('get_table_metadata_logical_id: get_table_metadata_logical_id Entering.')
    
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        
        logging.debug('get_table_metadata_logical_id: process name {0}.'.format(process_name))
        logging.debug('get_table_metadata_logical_id: vendor name {0}.'.format(vendor_name))
        logging.debug('get_table_metadata_logical_id: schema name {0}.'.format(destination_target.get_schema_name()))

        params = (process_name, vendor_name, destination_target.get_schema_name())
        crsr = conn.cursor()
        crsr.execute(f"DECLARE @output INT; EXEC {destination_target.get_database_name()}.[dbo].[SHS_SP_SRC_TABLE_METADATA] @source_table_name = ?, @source_database_name = ?, @source_schema_name = ?, @logical_id = @output OUTPUT; SELECT @output;", params)
        ret = crsr.fetchval()

    logging.info('get_table_metadata_logical_id: table metadata logical id {0}.'.format(ret))
    logging.info('get_table_metadata_logical_id: get_table_metadata_logical_id Leaving.')
    return ret

#TODO: Create unit test
def check_stg_table_exists(process_name, vendor_name, destination_target):
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        stg_table_name = f'STG_{vendor_name.upper()}_{process_name.upper()}'
        stg_exists_query = f"SELECT COUNT(1) FROM {destination_target.get_database_name()}.sys.tables t WHERE t.name = '{stg_table_name}'"
        crsr.execute(stg_exists_query)
        return bool(crsr.fetchval())

#TODO: Create unit test
def create_stg_table(process_name, vendor_name, destination_target, columns, primary_keys=None): 
    
    logger = logging.getLogger()

    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        logger.info('Stage table {0} does not exist. Creating...'.format(create_stg_table_name(process_name=process_name, vendor_name=vendor_name)))

        crsr = conn.cursor()

        create_stg_table = f"EXEC {destination_target.get_database_name()}.[dbo].[SHS_SP_CREATE_STG_TABLE] @table_name = ?, @database_name = ?, @table_primary_keys = ?, @table_columns = ?"
        create_stg_table_params = (process_name, vendor_name, primary_keys, columns)

        crsr.execute(create_stg_table, create_stg_table_params)
        crsr.commit()

        # check that stage table was created successfully
        if not check_stg_table_exists(
                process_name=process_name, 
                vendor_name=vendor_name, 
                destination_target=destination_target
            ):
            # new exception, exit code
            message = f'Stage table {0} failed to create table.'
            logger.error(message)
            sys.exit(message)

        logger.info('Stage table {0} created.'.format(create_stg_table_name(process_name=process_name, vendor_name=vendor_name))) 

#TODO: Create unit test
def check_psa_table_exists(process_name, vendor_name, destination_target):
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        psa_table_name = f'PSA_{vendor_name.upper()}_{process_name.upper()}'
        psa_exists_query = f"SELECT COUNT(1) FROM {destination_database}.sys.tables t WHERE t.name = '{psa_table_name}'"
        crsr.execute(psa_exists_query)
        return bool(crsr.fetchval())

#TODO: Create unit test
def create_psa_table(process_name, vendor_name, destination_target, columns, primary_keys=None):
    
    logger = logging.getLogger()
    
    with pyodbc.connect(destination_target.create_connection_string()) as conn:

        logger.info('Persistant Stage table {0} does not exist. Creating...'.format(create_psa_table_name(process_name=process_name, vendor_name=vendor_name)))
        
        crsr = conn.cursor()

        create_psa_table = f"EXEC {destination_target.get_database_name()}.[dbo].[SHS_SP_CREATE_PSA_TABLE] @table_name = ?, @database_name = ?, @table_primary_keys = ?, @table_columns = ?"
        create_psa_table_params = (process_name, vendor_name, primary_keys, columns)
        
        crsr.execute(create_psa_table, create_psa_table_params)
        crsr.commit()
        
        # check that psa table was created successfully
        if not check_psa_table_exists(process_name=process_name, vendor_name=vendor_name, destination_target=destination_target):
            # new exception, exit code
            message = f'Persistant stage table {0} failed to create table.'
            logger.error(message)
            sys.exit(message)

        logger.info('Persistant Stage table {0} created.'.format(create_psa_table_name(process_name=process_name, vendor_name=vendor_name)))

#TODO: Create unit test
def check_table_metadata(process_name, vendor_name, destination_target):
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        stg_table_name = f'STG_{vendor_name.upper()}_{process_name.upper()}'
        psa_table_name = f'PSA_{vendor_name.upper()}_{process_name.upper()}'
        
        logging.debug('check_table_metadata: Checking table metadata for {0} and {1}.'.format(stg_table_name, psa_table_name))
        
        crsr.execute("SELECT COUNT(1) FROM dbo.TABLE_METADATA WHERE STG_TABLE_NAME = '{0}' AND PSA_TABLE_NAME = '{1}'".format(stg_table_name, psa_table_name))
        
        tables_exist = bool(crsr.fetchval())

        return tables_exist

def update_table_metadata(process_name, vendor_name, destination_target):

    logging.info('update_table_metadata: Entering.')

    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()    
        stg_table_name = f'STG_{vendor_name.upper()}_{process_name.upper()}'
        psa_table_name = f'PSA_{vendor_name.upper()}_{process_name.upper()}'
        
        query = "SELECT COUNT(1) FROM dbo.TABLE_METADATA WHERE STG_TABLE_NAME = '{0}' AND PSA_TABLE_NAME = '{1}' AND BATCH_SIZE {2}".format(stg_table_name, psa_table_name, 'IS NULL' if destination_target.get_psa_batch_size() is None else '<> {0}'.format(destination_target.get_psa_batch_size()))
        crsr.execute(query)
        update_required = bool(crsr.fetchval())
        logging.debug('update_table_metadata: {0}.'.format(query))
        
        if not update_required:
            logging.info('update_table_metadata: Updating table metadata for {0} and {1}.'.format(stg_table_name, psa_table_name))
            query = "UPDATE dbo.TABLE_METADATA SET BATCH_SIZE = {0} WHERE STG_TABLE_NAME = '{1}' AND PSA_TABLE_NAME = '{2}'".format(destination_target.get_psa_batch_size(), stg_table_name, psa_table_name)
            crsr.execute(query)
            crsr.commit()
            logging.debug('update_table_metadata: {0}.'.format(query))

    logging.info('update_table_metadata: Leaving.')

#TODO: Create unit test
def truncate_stage_table(process_name, vendor_name, destination_target):
    logging.info('truncate_stage_table Entering.')
    with pyodbc.connect(destination_target.create_connection_string()) as conn:
        crsr = conn.cursor()
        stg_table_name = create_stg_table_name(
            process_name=process_name.upper(),
            vendor_name=vendor_name.upper()
        )
        logging.debug('truncate_stage_table: Truncate Stage table {0} created.'.format(stg_table_name))
        crsr.execute("TRUNCATE TABLE stg.{0}".format(stg_table_name))

    logging.info('truncate_stage_table Leaving.')


#TODO: Create unit test
def load_psa(destination_target, logical_id):
    """Process that calls psa.SHS_SP_PSA_LOAD_TABLE on destination target to load data from stage to psa table. 
    
        Keyword arguments:
        destination_target - Sql server object representing the destination (extract_sql_server.SqlServerTarget).
        logical_id - Logical id for the table metadata from dbo.TABLE_METADATA (integer).     
    """

    logger.info("load_psa: Entering.")

    # execute SHS_SP_PSA_LOAD
    try:
        with pyodbc.connect(destination_target.create_connection_string()) as conn:
            crsr = conn.cursor()
            crsr.execute("EXEC psa.SHS_SP_PSA_LOAD_TABLE @LogicalId = ?", logical_id)
            crsr.commit()
    except pyodbc.Error as e: 
        logger.error(f'{e}')
        raise Exception(e)
    except Exception as e: 
        logger.error(f'Unexpexted error: {e}')
        raise Exception(e)

    logger.info("load_psa: Leaving.")

def create_stg_table_name(process_name, vendor_name): 
    """Create a stage table name for the given process and vendor name.

        Keyword arguments:
        process_name - The process name for this stage table (i.e. source table name). (string) 
        vendor_name - The vendor name for this stage table (i.e. source database name). (string)
    """
    return 'STG_{0}_{1}'.format(vendor_name, process_name)

def create_psa_table_name(process_name, vendor_name): 
    """Create a persistant stage table name for the given process and vendor name.

        Keyword arguments:
        process_name - The process name for this stage table (i.e. source table name). (string) 
        vendor_name - The vendor name for this stage table (i.e. source database name). (string)
    """
    return 'PSA_{0}_{1}'.format(vendor_name, process_name)

def delete_file(path):
    logger.info('delete_file Entering.')
    logger.info('Delete file {0}'.format(str(path)))
    os.remove(path)
    logger.info('delete_file Leaving.')

def archive_file(path, new_path):
    logger.info('archive_file Entering.')
    logger.info('Archive file {0}'.format(str(path)))
    logger.info('New location file {0}'.format(str(new_path)))
    os.rename(path, new_path)
    logger.info('archive_file Leaving.')

def get_data_source_id(process_name, vendor_name, destination_target):
    
    data_source_id = 0

    try:
        with pyodbc.connect(destination_target.create_connection_string()) as conn:
            crsr = conn.cursor()
            crsr.execute("SELECT ds.DATA_SOURCE_ID FROM dbo.DATA_SOURCE ds WHERE ds.DATA_SOURCE_NAME = UPPER('{0}')".format(vendor_name))
            data_source_id = crsr.fetchval()
    except pyodbc.Error as e: 
        logger.error(f'{e}')
        raise Exception(e)
    except Exception as e: 
        logger.error(f'Unexpexted error: {e}')
        raise Exception(e)

    return data_source_id


# extract types
#TODO: Create unit test
def extract_mssql(source, log_queue):
    """Process which extracts SQL Server tables in the extract load process. 

        Keyword arguments:
        source - The SQL Server target object representing the source table. (extract_sql_server.SqlServerTarget) 
        log_queue - The multiprocessing queue object for recording logging evengs. (Multiprocessing.Queue) 
    """
    
    logger = logging.getLogger()

    source_database = source['database']
    source_server = source['server']
    source_schema = source['schema']
    # source_chunk_size = source["chunk_size"]

    logger.debug("Source database: {0}".format(source_database))
    logger.debug("Source server: {0}".format(source_server))
    logger.debug("Source schema: {0}".format(source_schema))

    logger.debug("Start of tables.")

    for table_config in source['tables']:

        table = table_config['name'] 

        if 'psa_batch_size' in table_config:        
            psa_batch_size = table_config['psa_batch_size'] 
        else:
            psa_batch_size = None

        # determine whether to leverage PSA only or STG
        if 'psa_only' in table_config: 
            psa_only = True
        else: 
            psa_only = False

        logger.debug("Source table: {0}".format(table))

        source_target = SqlServerTarget(source_server, source_database, table, source_schema)

        if psa_only: 
            destination_table = create_psa_table_name(
                    source_target.get_table_name().upper(),
                    source_target.get_database_name().upper() 
                )
        else: 
            destination_table = create_stg_table_name(
                    source_target.get_table_name().upper(),
                    source_target.get_database_name().upper() 
                )
        
        # create destination target
        destination_target = SqlServerTarget(
                server=destination_server, 
                database=destination_database, 
                table=destination_table, 
                schema=destination_schema if not psa_only else 'psa',
                psa_batch_size=psa_batch_size
        )

        # check that table exists on source 
        if not source_target.check_table_exists():
            logger.warn('Table {0} does not exist on in the source.'.format(source_target.get_table_name()))
            break

        with pyodbc.connect(destination_target.create_connection_string()) as conn: 
            crsr = conn.cursor() 

            # create stage/psa tables if not exists
            if not psa_only:
                if not check_stg_table_exists(process_name=source_target.get_table_name(), vendor_name=source_target.get_database_name(), destination_target=destination_target):
                    create_stg_table(
                        process_name=source_target.get_table_name(), 
                        vendor_name=source_target.get_database_name(), 
                        destination_target=destination_target, 
                        columns=column_metadata_to_json(source_target.get_columns()), 
                        primary_keys=column_metadata_to_json(source_target.get_primary_keys())
                    )

            if not check_psa_table_exists(process_name=source_target.get_database_name(), vendor_name=source_target.get_table_name(), destination_target=destination_target):
                create_psa_table(
                    process_name=source_target.get_table_name(), 
                    vendor_name=source_target.get_database_name(), 
                    destination_target=destination_target, 
                    columns=column_metadata_to_json(source_target.get_columns()), 
                    primary_keys=column_metadata_to_json(source_target.get_primary_keys())
                )

            # truncate stage
            if not psa_only:
                truncate_stage_table(process_name=source_target.get_table_name(), vendor_name=source_target.get_database_name(), destination_target=destination_target)

            # process table metadata
            if not check_table_metadata(process_name=source_target.get_table_name(), vendor_name=source_target.get_database_name(), destination_target=destination_target):
                # create new record and return logical id
                logger.info('Creating table metadata record for {0}.'.format(table))
                source_target.set_table_metadata_logical_id(
                    insert_table_metadata_logical_id(
                        process_name=source_target.get_table_name(), 
                        vendor_name=source_target.get_database_name(), 
                        destination_target=destination_target
                    )
                )
            else: 
                # get logical id for existing record
                logger.info('Get table metadata record for {0}.'.format(table))
                update_table_metadata(
                    process_name=source_target.get_table_name(),
                    vendor_name=source_target.get_database_name(),
                    destination_target=destination_target
                )

                source_target.set_table_metadata_logical_id(
                    get_table_metadata_logical_id(
                        process_name=source_target.get_table_name(),
                        vendor_name=source_target.get_database_name(),
                        destination_target=destination_target
                    )
                )

            # check that change tracking exists on the current table
            if not source_target.check_change_tracking():
                logger.info('Change tracking not enabled on table {0}. Enabling...'.format(table))
                # enable change tracking on table
                source_target.add_change_tracking()
                if not source_target.check_change_tracking():
                    logger.error('Failed to enable change tacking on table {0}.'.format(table))
                    sys.exit(-1)
                print('Change tracking enabled on table {0}.'.format(table))

            # get new change key
            source_target.set_current_change_key(source_target.get_new_change_tracking_key())

            # add change record to database 
            source_target.set_current_source_change_log_id(insert_source_change_log(source_target=source_target, destination_target=destination_target))

            # get previous change record
            source_target.set_previous_change_key(get_previous_change_tracking_key(source_target=source_target, destination_target=destination_target))

        # begin processing
        conn = source_target.create_engine(isolation_level="SNAPSHOT")

        data_source_id = get_data_source_id(process_name=source_target.get_table_name(), vendor_name=source_target.get_database_name(), destination_target=destination_target)

        # create a column listing to handle max date limitations in Pandas
        column_list_record_hash = '' # DEVOPS#3442
        column_list = ''
        columns = source_target.get_columns()

        for index, column in enumerate(columns):
            # this is required because pandas dataframe has a max date of 4/11/2262 -- MAC 2020-05-18 
            if column[2] == 'datetime':
                column_list += f"CASE WHEN t.[{column[1]}] > '4/11/2262' THEN NULL ELSE t.[{column[1]}] END [{column[1]}]"
                column_list_record_hash += f"CASE WHEN t.[{column[1]}] > '4/11/2262' THEN NULL ELSE t.[{column[1]}] END" # DEVOPS#3442 -- no column name  
            elif column[2] == 'timestamp': 
                column_list += f"CONVERT(VARCHAR(MAX), CONVERT(BINARY(8), [{column[1]}]), 1) {column[1]}"
                column_list_record_hash += f"CONVERT(VARCHAR(MAX), CONVERT(BINARY(8), [{column[1]}]), 1)" # DEVOPS#3442 -- no column name  
            else:
                column_list += f"t.[{column[1]}]"
                column_list_record_hash += f"t.[{column[1]}]" # DEVOPS#3442
            
            if not (len(columns)-1) == index:
                column_list += ', '
                column_list_record_hash += ', ' # DEVOPS#3442

        if source_target.get_previous_change_key() is not None:
            primary_key_list = ''
            primary_key_list_record_uid = ''
            primary_key_predicate = ''
            primary_keys = source_target.get_primary_keys()
            for index, key in enumerate(primary_keys):
                primary_key_list += f'{key[1]}'
                primary_key_list_record_uid += f't.[{key[1]}]'
                primary_key_predicate += f'ct.[{key[1]}] = t.[{key[1]}]'
                if not (len(primary_keys)-1) == index:
                    primary_key_list += ','
                    primary_key_list_record_uid += ','
                    primary_key_predicate += ' AND '
            
            if len(primary_keys) > 1: 
                primary_key_list_record_uid = 'CONCAT({0})'.format(primary_key_list_record_uid)
            else: 
                primary_key_list_record_uid = 'CONVERT(VARCHAR(MAX), {0})'.format(primary_key_list_record_uid)

            if psa_only: 
                query = f"""SELECT HASHBYTES('SHA1', CONVERT(VARCHAR(MAX), NEWID())) HPXR_UID, HASHBYTES('SHA1', {primary_key_list_record_uid}) RECORD_UID, HASHBYTES('SHA1', CONCAT({column_list_record_hash})) RECORD_HASH, {data_source_id} DATA_SOURCE_ID, {column_list} FROM {source_target.get_database_name()}.{source_target.get_schema_name()}.{source_target.get_table_name()} t JOIN (SELECT {primary_key_list} FROM CHANGETABLE(CHANGES {source_target.get_schema_name()}.{source_target.get_table_name()}, {source_target.get_previous_change_key()}) ct ) ct ON {primary_key_predicate}"""
            else:
                query = f"""SELECT {column_list} FROM {source_target.get_database_name()}.{source_target.get_schema_name()}.{source_target.get_table_name()} t JOIN (SELECT {primary_key_list} FROM CHANGETABLE(CHANGES {source_target.get_schema_name()}.{source_target.get_table_name()}, {source_target.get_previous_change_key()}) ct ) ct ON {primary_key_predicate}"""
        else: 
            if psa_only:
                query = f"""SELECT HASHBYTES('SHA1', CONVERT(VARCHAR(MAX), NEWID())) HPXR_UID, HASHBYTES('SHA1', CONCAT({column_list_record_hash})) RECORD_UID, HASHBYTES('SHA1', CONCAT({column_list_record_hash})) RECORD_HASH, {data_source_id} DATA_SOURCE_ID, {column_list} FROM {source_target.get_database_name()}.{source_target.get_schema_name()}.{source_target.get_table_name()} t"""
            else: 
                query = f"""SELECT {column_list} FROM {source_target.get_database_name()}.{source_target.get_schema_name()}.{source_target.get_table_name()} t"""

        generator = pd.read_sql_query(query, conn, chunksize=(25000 if source_target.get_chunk_size() is None else source_target.get_chunk_size()))

        extract_load(
            generator=generator, 
            log_queue=log_queue, 
            destination_target=destination_target,
            is_chunks=True,
            psa_only=psa_only
        )

        #TODO: Once all tables converted to psa_only, remove this
        if not psa_only:
            load_psa(destination_target=destination_target, logical_id=source_target.get_table_metadata_logical_id())
        
        # update source change log set to complete 
        with pyodbc.connect(destination_target.create_connection_string()) as conn:
            crsr = conn.cursor()
            crsr.execute("EXEC dbo.SHS_SP_UPD_SOURCE_CHANGE_LOG @change_log_id = ?", source_target.get_current_source_change_log_id())
        
    
    logger.debug("End of tables.")

#TODO: Create unit test
EXCEL_EXTENSIONS = [ "xls", "xlsx", "xlsm", "xlsb", "obf" ]
def extract_excel(source, log_queue):
    """Process which extracts Excel files in the extract load process. 

        Keyword arguments:
        source - The configuration for the source files.  
        log_queue - The multiprocessing queue object for recording logging evengs. (Multiprocessing.Queue) 
    """
    logger = logging.getLogger()

    # check that the files key exists within the configuration
    if not 'files' in source:
        logger.warn('No files to process.')

    file_iterator = 1

    for file_configuration in source['files']: 
        excel = ExcelTarget(file_configuration)
            
        logger.debug('primary_key: {0}'.format(excel.get_primary_key()))

        df = pd.DataFrame()

        # create destination target
        destination_target = SqlServerTarget(
                server=destination_server, 
                database=destination_database, 
                table=create_stg_table_name(
                    excel.get_process_name().upper(),
                    excel.get_vendor_name().upper() 
                ), 
                schema=destination_schema,
                psa_batch_size=excel.get_psa_batch_size()
        )

        excel_files = [] 

        # EXCEL_EXTENSIONS: xls, xlsx, xlsm, xlsb, and odf
        for ext in EXCEL_EXTENSIONS:
            for file in glob.glob(os.path.join(excel.get_path(), "*.{0}".format(ext))):
                excel_files.append(file)

        for file in excel_files:

            df = pd.read_excel(
                io=os.path.join(excel.get_path(), file), 
                dtype=str,
                skiprows=excel.get_skip_rows()
            )

            # negative skip rows 
            if excel.get_skip_rows() < 0:
                df = df[:(len(df) + excel.get_skip_rows())]

            #TODO: Check to make sure file will load to table, file schema matches table schema
            logger.info('Processing file {0}'.format(file))
            
            # check stage/psa tables exist
            if not check_stg_table_exists(
                process_name=excel.get_process_name(), 
                vendor_name=excel.get_vendor_name(), 
                destination_target=destination_target
            ) or not check_psa_table_exists(
                process_name=excel.get_process_name(), 
                vendor_name=excel.get_vendor_name(), 
                destination_target=destination_target
            ):
                
                logger.info('Destination tables missing...')
                
                # create dataframe of file columns
                data = {
                    'columnd_id' : range(2, len(df.columns) + 2), 
                    'column_name' : df.columns, 
                    'data_type' : 'varchar', 
                    'length' : 0,
                    'precision' : 0, 
                    'scale' : 0 }
                column_df = pd.DataFrame(data, dtype=str)

                # add metadata 
                metadata = {
                    'columnd_id' : 1, 
                    'column_name' : 'HPXR_FILE_NAME', 
                    'data_type' : 'varchar', 
                    'length' : 0,
                    'precision' : 0, 
                    'scale' : 0 }
                metadata_df = pd.DataFrame(metadata, index=[0], dtype=str)

                column_df = metadata_df.append(column_df, ignore_index=True)
                
                if excel.get_primary_key():
                    primary_key_df = pd.DataFrame(dtype=str)
                    
                    for index, key in enumerate(primary_keys):
                        current_key_df = pd.DataFrame({ 'column_id' : (index + 1), 'column_name' : key['column_name'], 'data_type' : key['data_type'], 'length' : key['length'], 'precision' : 0, 'scale' : 0}, index=[0], dtype=str)
                        primary_key_df = primary_key_df.append(current_key_df, ignore_index=True)

                        # update the columns dataframe with metadata from primary key
                        current_key_in_columns = column_df.loc[column_df['column_name'] == key['column_name']]
                        current_key_in_columns['data_type'] = key['data_type']
                        current_key_in_columns['length'] = key['length']

                        column_df.update(current_key_in_columns)

                    primary_keys = primary_key_df.to_json(orient='records')


                columns = column_df.to_json(orient='records')
                
                logger.debug(columns)
                logger.debug(excel.get_primary_key())

                create_stg_table(
                    process_name=excel.get_process_name(), 
                    vendor_name=excel.get_vendor_name(), 
                    destination_target=destination_target, 
                    columns=columns, 
                    primary_keys=excel.get_primary_key()
                )

                create_psa_table(
                    process_name=excel.get_process_name(), 
                    vendor_name=excel.get_vendor_name(), 
                    destination_target=destination_target, 
                    columns=columns, 
                    primary_keys=excel.get_primary_key()
                )

            # truncate stage
            truncate_stage_table(process_name=excel.get_process_name(), vendor_name=excel.get_vendor_name(), destination_target=destination_target)

            # process table metadata
            if not check_table_metadata(
                process_name=excel.get_process_name(), 
                vendor_name=excel.get_vendor_name(), 
                destination_target=destination_target
            ):
                # create new record and return logical id
                logger.info('Creating table metadata record for {0}.'.format(create_stg_table_name(process_name=excel.get_process_name(), vendor_name=excel.get_vendor_name())))
                destination_target.set_table_metadata_logical_id(
                    insert_table_metadata_logical_id(
                        process_name=excel.get_process_name(), 
                        vendor_name=excel.get_vendor_name(), 
                        destination_target=destination_target
                    )
                )
            else: 
                # get logical id for existing record
                logger.info('Get table metadata record for {0}.'.format(create_stg_table_name(process_name=excel.get_process_name(), vendor_name=excel.get_vendor_name())))
                update_table_metadata(
                    process_name=excel.get_process_name(),
                    vendor_name=excel.get_vendor_name(),
                    destination_target=destination_target
                )

                destination_target.set_table_metadata_logical_id(
                    get_table_metadata_logical_id(
                        process_name=excel.get_process_name(), 
                        vendor_name=excel.get_vendor_name(), 
                        destination_target=destination_target
                    )
                )

            file_iterator += 1

            logger.info('Writing records to table {0}'.format(destination_target.get_table_name()))

            df['HPXR_FILE_NAME'] = os.path.basename(file)
            
            df.to_sql(name=destination_target.get_table_name(), index=False, schema=destination_target.get_schema_name(), if_exists='append', con=destination_target.create_engine())

            if excel.get_is_delete_file():
                delete_file(os.path.join(excel.get_path(), file))
                
            if excel.get_is_archive_file():
                archive_file(
                    os.path.join(excel.get_path(), file), 
                    os.path.join(excel.get_archive_file_path(), file)
                )
                delete_file(os.path.join(excel.get_path(), file))

            load_psa(
                destination_target=destination_target, 
                logical_id=destination_target.get_table_metadata_logical_id()
            )

#TODO: Create unit test
CSV_EXTENSIONS = [ "txt", "csv" ]
def extract_csv(source, log_queue): 
    """Process which extracts CSV files in the extract load process. 

        Keyword arguments:
        source - The configuration for the source files.  
        log_queue - The multiprocessing queue object for recording logging evengs. (Multiprocessing.Queue) 
    """
    logger = logging.getLogger()

    # check that the files key exists within the configuration
    if not 'files' in source:
        logger.warn('No files to process.')

    file_iterator = 1

    for file_configuration in source['files']: 
        csv = CsvTarget(file_configuration)
            
        logger.debug('primary_key: {0}'.format(csv.get_primary_key()))

        df = pd.DataFrame()

        # create destination target
        destination_target = SqlServerTarget(
                server=destination_server, 
                database=destination_database, 
                table=create_stg_table_name(
                    csv.get_process_name().upper(),
                    csv.get_vendor_name().upper() 
                ), 
                schema=destination_schema,
                psa_batch_size=csv.get_psa_batch_size()
        )

        csv_files = [] 

        # EXCEL_EXTENSIONS: xls, xlsx, xlsm, xlsb, and odf
        for ext in CSV_EXTENSIONS:
            for file in glob.glob(os.path.join(csv.get_path(), "*.{0}".format(ext))):
                csv_files.append(file)

        for file in csv_files:

            df = pd.read_csv(
                os.path.join(csv.get_path(), file),
                sep='|',
                dtype=str
            )

            columns = df.columns

            #TODO: Check to make sure file will load to table, file schema matches table schema
            logger.info('Processing file {0}'.format(file))
            
            # check stage/psa tables exist
            if not check_stg_table_exists(
                process_name=csv.get_process_name(), 
                vendor_name=csv.get_vendor_name(), 
                destination_target=destination_target
            ) or not check_psa_table_exists(
                process_name=csv.get_process_name(), 
                vendor_name=csv.get_vendor_name(), 
                destination_target=destination_target
            ):
                
                logger.info('Destination tables missing...')
                
                # create dataframe of file columns
                data = {
                    'columnd_id' : range(2, len(df.columns) + 2), 
                    'column_name' : df.columns, 
                    'data_type' : 'varchar', 
                    'length' : 0,
                    'precision' : 0, 
                    'scale' : 0 }
                column_df = pd.DataFrame(data, dtype=str)

                # add metadata 
                metadata = {
                    'columnd_id' : 1, 
                    'column_name' : 'HPXR_FILE_NAME', 
                    'data_type' : 'varchar', 
                    'length' : 0,
                    'precision' : 0, 
                    'scale' : 0 }
                metadata_df = pd.DataFrame(metadata, index=[0], dtype=str)

                column_df = metadata_df.append(column_df, ignore_index=True)
                
                if csv.get_primary_key():
                    primary_key_df = pd.DataFrame(dtype=str)
                    
                    for index, key in enumerate(primary_keys):
                        current_key_df = pd.DataFrame({ 'column_id' : (index + 1), 'column_name' : key['column_name'], 'data_type' : key['data_type'], 'length' : key['length'], 'precision' : 0, 'scale' : 0}, index=[0], dtype=str)
                        primary_key_df = primary_key_df.append(current_key_df, ignore_index=True)

                        # update the columns dataframe with metadata from primary key
                        current_key_in_columns = column_df.loc[column_df['column_name'] == key['column_name']]
                        current_key_in_columns['data_type'] = key['data_type']
                        current_key_in_columns['length'] = key['length']

                        column_df.update(current_key_in_columns)

                    primary_keys = primary_key_df.to_json(orient='records')


                columns = column_df.to_json(orient='records')
                
                logger.debug(columns)
                logger.debug(csv.get_primary_key())

                create_stg_table(
                    process_name=csv.get_process_name(), 
                    vendor_name=csv.get_vendor_name(), 
                    destination_target=destination_target, 
                    columns=columns, 
                    primary_keys=csv.get_primary_key()
                )

                create_psa_table(
                    process_name=csv.get_process_name(), 
                    vendor_name=csv.get_vendor_name(), 
                    destination_target=destination_target, 
                    columns=columns, 
                    primary_keys=csv.get_primary_key()
                )

            # truncate stage
            truncate_stage_table(process_name=csv.get_process_name(), vendor_name=csv.get_vendor_name(), destination_target=destination_target)

            # process table metadata
            if not check_table_metadata(
                process_name=csv.get_process_name(), 
                vendor_name=csv.get_vendor_name(), 
                destination_target=destination_target
            ):
                # create new record and return logical id
                logger.info('Creating table metadata record for {0}.'.format(create_stg_table_name(process_name=csv.get_process_name(), vendor_name=csv.get_vendor_name())))
                destination_target.set_table_metadata_logical_id(
                    insert_table_metadata_logical_id(
                        process_name=csv.get_process_name(), 
                        vendor_name=csv.get_vendor_name(), 
                        destination_target=destination_target
                    )
                )
            else: 
                # get logical id for existing record
                logger.info('Get table metadata record for {0}.'.format(create_stg_table_name(process_name=csv.get_process_name(), vendor_name=csv.get_vendor_name())))
                update_table_metadata(
                    process_name=csv.get_process_name(),
                    vendor_name=csv.get_vendor_name(),
                    destination_target=destination_target
                )

                destination_target.set_table_metadata_logical_id(
                    get_table_metadata_logical_id(
                        process_name=csv.get_process_name(), 
                        vendor_name=csv.get_vendor_name(), 
                        destination_target=destination_target
                    )
                )

            file_iterator += 1

            logger.info('Writing records to table {0}'.format(destination_target.get_table_name()))

            df['HPXR_FILE_NAME'] = os.path.basename(file)
            
            df.to_sql(name=destination_target.get_table_name(), index=False, schema=destination_target.get_schema_name(), if_exists='append', con=destination_target.create_engine())

            if csv.get_is_delete_file():
                delete_file(os.path.join(csv.get_path(), file))
                
            if csv.get_is_archive_file():
                archive_file(
                    os.path.join(csv.get_path(), file), 
                    os.path.join(csv.get_archive_file_path(), file)
                )
                delete_file(os.path.join(csv.get_path(), file))

            load_psa(
                destination_target=destination_target, 
                logical_id=destination_target.get_table_metadata_logical_id()
            )

if __name__ == "__main__":

    if len(sys.argv) >= 2: 
        if sys.argv[1] != None:
            configuration_path = sys.argv[1]
    else:
        configuration_path = r"configuration.json"

    if not configuration_path == None: 
        with open(configuration_path, "rb") as file:
            try: 
                configuration = json.loads(file.read())
            except Exception as ex: 
                print(f'extract_load.main: Failed loading configuration {configuration_path}.')
                print(f'extract_load.main: {ex}')

    # create extract logger
    logging_file_name = configuration['logging']['file_name']
    logging_file_path = configuration['logging']['file_path']
    logging_level = configuration['logging']['log_level']

    logging_full_file_name = logging_file_path + logging_file_name

    queue = multiprocessing.Queue(-1)
    listener = multiprocessing.Process(target=extract_log.listener, args=(queue, extract_log.listener_configurer, logging_full_file_name, True))
    listener.start()

    extract_log.worker_configurer(queue)
    logger = logging.getLogger()

    # get destination configuration settings
    destination_server = configuration['destination']['server']
    destination_database = configuration['destination']['database']
    destination_schema = configuration['destination']['schema']

    logger.debug("Start of sources.")

    # loop through sources
    for source in configuration['source']:
        # what type of source are we dealing with?
        source_type = source["type"]

        logger.debug('Source type: {0}'.format(source_type))

        if source_type == 'mssql':
            extract_mssql(source, log_queue=queue)

        if source_type == 'excel':
            extract_excel(source, log_queue=queue)

        if source_type == 'csv': 
            extract_csv(source, log_queue=queue)
        
    logger.debug("End of sources.")
    # terminate logging
    queue.put(None)
    queue.cancel_join_thread()
    listener.join()
    listener.close()

    sys.exit(0)    