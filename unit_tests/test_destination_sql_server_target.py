import sys, os 
import unittest 
import json
import pyodbc
import pandas as pd
import threading
import queue

from targets.destination_sql_server_target import DestinationSqlServerTarget

class TestDestinationSqlServerTarget(unittest.TestCase):
    
    def setUp(self):
        self._configuration = json.loads('{ "server" : "DEVSQL17TRZRP", "database" : "hpXr_Stage", "schema" : "dbo" }')
        self._destination_sql_server_target = DestinationSqlServerTarget(self._configuration["server"], self._configuration["database"], self._configuration["schema"], self._configuration["load_psa"])

    def test_check_psa_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.check_psa_destination_table("TEST_CMC_CLCL_CLAIM", "facets"))

    def test_check_stg_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.check_stg_destination_table("TEST_CMC_CLCL_CLAIM", "facets"))

    def test_create_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.create_destination_table("CMC_UMSV_SERVICES", "facets"))

    def test_set_change_tracking_key(self):
        self.assertTrue(self._destination_sql_server_target.set_change_tracking_key("TEST_CMC_CLCL_CLAIM", "facets", 100))

    def test_get_previous_change_tracking_key(self):
        self.assertIsNotNone(self._destination_sql_server_target.get_previous_change_tracking_key("TEST_CMC_CLCL_CLAIM", "facets"))
