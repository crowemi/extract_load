import sys, os 
import unittest 
import json
import pyodbc

from targets.destination_sql_server_target import DestinationSqlServerTarget

class TestDestinationSqlServerTarget(unittest.TestCase):
    
    def setUp(self):
        self._configuration = json.loads('{ "server" : "DEVSQL17TRZRP", "database" : "hpXr_Stage" }')
        self._destination_sql_server_target = DestinationSqlServerTarget(self._configuration)

    def test_check_psa_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.check_psa_destination_table("TEST_CMC_CLCL_CLAIM", "facets"))

    def test_check_stg_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.check_stg_destination_table("TEST_CMC_CLCL_CLAIM", "facets"))

    def test_create_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.create_destination_table("TEST_CMC_CLCL_CLAIM", "facets"))


