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
        self._configuration = json.loads('{ "server" : "DEVSQL17TRZRP", "database" : "hpXr_Stage" }')
        self._destination_sql_server_target = DestinationSqlServerTarget(self._configuration)

    def test_check_psa_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.check_psa_destination_table("TEST_CMC_CLCL_CLAIM", "facets"))

    def test_check_stg_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.check_stg_destination_table("TEST_CMC_CLCL_CLAIM", "facets"))

    def test_create_destination_table(self):
        self.assertTrue(self._destination_sql_server_target.create_destination_table("TEST_CMC_CLCL_CLAIM", "facets"))

    def test_set_change_tracking_key(self):
        self.assertTrue(self._destination_sql_server_target.set_change_tracking_key("TEST_CMC_CLCL_CLAIM", "facets", 100))

    def test_get_previous_change_tracking_key(self):
        self.assertIsNotNone(self._destination_sql_server_target.get_previous_change_tracking_key("TEST_CMC_CLCL_CLAIM", "facets"))

    def test_exec_sp(self):
        server = 'DEVSQL17TRZ3'
        database = 'facets'
        # connection_string =f"DRIVER={{ODBC Driver 17 for SQL Server}}; SERVER=DEVSQL17TRZRP; DATEBASE=hpXr_Stage; Trusted_Connection=yes"
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+'; Trusted_Connection=yes'
        conn = pyodbc.connect(connection_string)
        # create_stg_table = f"EXEC hpXr_Stage.[dbo].[SHS_SP_CREATE_STG_TABLE] @table_name = ?, @source_database_name = ?"
        # params = ("TEST_CMC_CLCL_CLAIM", "facets")
        sql = "SELECT * FROM CMC_CLCL_CLAIM"
        crsr = conn.cursor()
        
        # ret = crsr.execute("SELECT * FROM CMC_CLCL_CLAIM").fetchall()
        # val = ret
        
        for row in crsr.execute(sql):
            print(row)
        
        # df = pd.read_sql(sql, conn)
        # json = df.loc[0].to_json()
        # print(json)

        # pd.to_sql()

        # crsr = conn.cursor()
        # try:
        #     crsr.execute("{CALL hpXr_Stage.dbo.SHS_SP_CREATE_STG_TABLE (?,?)}", params)
        #     crsr.commit()
        # except:
        #     print("error")            
        # ret1 = cursor.execute(create_stg_table, create_stg_table_params)

