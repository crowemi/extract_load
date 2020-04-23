import sys, os 
import unittest
import json 

from targets.source_sql_server_target import SourceSqlServerTarget

class TestSourceSqlServerTarget(unittest.TestCase):
    
    def setUp(self):
        self._configuration = json.loads('{ "server" : "DEVSQL17TRZ3", "database" : "facets", "schema" : "dbo", "tables" : [ "CMC_CLCL_CLAIM" ] }')
        self._source_sql_server_target = SourceSqlServerTarget(self._configuration["server"], self._configuration["database"], self._configuration["schema"], self._configuration["tables"][0])

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
        pass

    def test_get_records(self):
        pass

    def test_load_records(self):
        pass
    
    def test_get_primary_key(self):
        self.assertIsNotNone(self._source_sql_server_target._get_primary_keys())
        