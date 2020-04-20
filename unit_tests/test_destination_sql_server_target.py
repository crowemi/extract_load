import sys, os 
import unittest 
import json

from targets.destination_sql_server_target import DestinationSqlServerTarget

class TestDestinationSqlServerTarget(unittest.TestCase):
    
    def setUp(self):
        configuration = json.loads('{ "server" : "DEVSQL17TRZRP", "database" : "hpXr_Stage" }')
        self._destination_sql_server_target = DestinationSqlServerTarget(configuration)

    def test_check_psa_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.check_psa_destination_table("CMC_CLCL_CLAIM", "facets"))

    def test_check_stg_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.check_stg_destination_table("CMC_CLCL_CLAIM", "facets"))

    def test_create_destination_table(self):
        pass