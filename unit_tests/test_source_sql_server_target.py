import sys, os, threading 
import unittest
import json 

from targets.source_sql_server_target import SourceSqlServerTarget
from targets.destination_sql_server_target import DestinationSqlServerTarget

class TestSourceSqlServerTarget(unittest.TestCase):
    
    def setUp(self):
        self._configuration = json.loads('{ "server" : "DEVSQL17TRZ3", "database" : "facets", "schema" : "dbo", "tables" : [ "CMC_UMSV_SERVICES" ] }')
        self._source_sql_server_target = SourceSqlServerTarget(self._configuration["server"], self._configuration["database"], self._configuration["schema"], self._configuration["tables"][0])
        self._destination_sql_server_target = DestinationSqlServerTarget("DEVSQL17TRZRP", "hpXr_Stage", "stg", True)

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
        
        get_change_records = threading.Thread(target=self._source_sql_server_target.get_change_records,args=(0,1))
        get_change_records.start()
        
        load_change_records = threading.Thread(target=self._destination_sql_server_target.load_records, args=(self._source_sql_server_target.get_table_name(), self._source_sql_server_target.get_database_name(), self._source_sql_server_target._records))
        load_change_records.start()
        
        threads = []

        for _ in range(10):
            get_records = threading.Thread(target=self._source_sql_server_target.get_records)
            get_records.start()
            threads.append(get_records)

        get_change_records.join()

        for thread in threads: 
            thread.join()

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