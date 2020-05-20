import sys, os, threading 
import unittest
import json 
import pandas as pd
import urllib
import asyncio

from sqlalchemy import create_engine 

from datetime import datetime
from targets.source_sql_server_target import SourceSqlServerTarget
from targets.destination_sql_server_target import DestinationSqlServerTarget

class TestSourceSqlServerTarget(unittest.TestCase):
    
    def setUp(self):
        self._configuration = json.loads('{ "server" : "DEVSQL17TRZ3", "database" : "facets", "schema" : "dbo", "tables" : [ "CMC_UMSV_SERVICES" ] }')
        self._destination_sql_server_target = DestinationSqlServerTarget("DEVSQL17TRZRP", "hpXr_Stage", "stg", True)
        self._source_sql_server_target = SourceSqlServerTarget(
            self._configuration["server"], 
            self._configuration["database"], 
            self._configuration["schema"], 
            self._configuration["tables"][0],
            self._destination_sql_server_target
            )

    def test_get_tables(self): 
        pass

    def test_check_change_tracking(self):
        pass

    def test_add_change_tracking(self):
        pass

    def test_get_new_change_tracking_key(self):
        key = self._source_sql_server_target.get_new_change_tracking_key()
        self.assertIsNotNone(key) 
     
    def test_get_change_records(self):

        self._destination_sql_server_target.create_destination_table(self._source_sql_server_target.get_table_name(), self._source_sql_server_target.get_database_name())

        self._source_sql_server_target.get_records()


        # loop = asyncio.get_event_loop()

        # try:
        #     loop.run_until_complete(self._source_sql_server_target.get_records())
        # finally:
        #     loop.close()

        # get_records = threading.Thread(target=self._source_sql_server_target.get_records, name="master-get-records")
        # get_records.start()

        # load_threads = [] 
        
        # for _ in range (3):
        #     load_records = threading.Thread(target=self._destination_sql_server_target.load_records, name="master-load-records", args=(self._source_sql_server_target.get_table_name(), self._source_sql_server_target.get_database_name(), self._source_sql_server_target._records))
        #     load_records.start()
        #     load_threads.append(load_records)
        
        # get_records.join()
        
        # for thread in load_threads:
        #     thread.join()

        print("All Done!")

    def test_get_records(self):
        pass

    def test_load_records(self):
        pass
    
    def test_get_primary_key(self):
        self.assertIsNotNone(self._source_sql_server_target._get_primary_keys())
        
    def test_format_select_primary_keys(self):
        self.assertIsNotNone(self._source_sql_server_target.format_select_primary_keys())

    def test_format_where_primary_keys(self):
        self.assertIsNotNone(self._source_sql_server_target.format_where_primary_keys(("I", "123", "456")))


    def test_chunksize (self): 

        params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=DEVSQL17TRZRP;DATABASE=hpXr_Stage;Trusted_Connection=yes")
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

        with self._source_sql_server_target._connection as conn:
            query = "select * from CMC_CLCL_CLAIM WHERE CLCL_ID = '101270213700'"
            df_chunk = pd.read_sql_query(query, conn, chunksize=1)
            for chunk in df_chunk:
                t = type(chunk['ATXR_SOURCE_ID'])
                d = pd.DataFrame()
                # d['CHANGE_DT'] = chunk.apply(lambda row: datetime.now(), axis=1)
                # d['METADATA'] = chunk.apply(lambda row: '', axis=1)
                # d['RECORD'] = chunk.apply(lambda row: row.to_json(date_format='iso'), axis=1)
                
                # m = chunk.select_dtypes(include=['datetime64', 'date', 'datetime'])
                z = chunk.filter(regex ='DATE|_DT|TIMESTAMP', axis =1 )
                d = chunk.select_dtypes(include=['datetime64', 'datetime', 'datetime64']).columns
                # d = chunk.filter(regex='DATE|_DT|TIMESTRAP',axis='columns').columns
                for col in d: 
                    chunk[col] = pd.to_datetime(chunk[col], errors='coerce')

                # chunk['CLCL_RELHP_FROM_DT'] = pd.to_datetime(chunk['CLCL_RELHP_FROM_DT'], errors='coerce')
                
                print(chunk.select_dtypes(include=['datetime64']))
                chunk.to_sql(name='STG_FACETS_CMC_UMSV_SERVICES2', index=False, schema='stg', if_exists='append', con=engine)
