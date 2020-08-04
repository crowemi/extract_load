import sys, os
import unittest
import logging
import multiprocessing
import pyodbc
import pandas as pd

import extract_load
from sources.extract_sql_server import SqlServerTarget 

class TestExtractLoad(unittest.TestCase):

    def setUp(self):
        self.process_name = 'TESTPROCESSNAME'
        self.vendor_name = 'TESTVENDORNAME'

    def test_create_stg_table_name(self):
        stage_table_name = extract_load.create_stg_table_name(process_name=self.process_name, vendor_name=self.vendor_name)
        self.assertEqual(stage_table_name, 'STG_{0}_{1}'.format(self.vendor_name, self.process_name))

    def test_insert_table_metadata_logical_id(self, delete_record=True):
        
        destination_table_name = extract_load.create_stg_table_name(
            process_name=self.process_name, 
            vendor_name=self.vendor_name
        )

        destination_target = SqlServerTarget(
            "DEVSQL17TRZRP", 
            "hpXr_Stage", 
            destination_table_name, 
            "stg"
        )

        logical_id = extract_load.insert_table_metadata_logical_id(
            process_name=self.process_name, 
            vendor_name=self.vendor_name, 
            destination_target=destination_target
        )
        
        table_name = ''

        with pyodbc.connect(destination_target.create_connection_string()) as conn:
            crsr = conn.cursor()
            crsr.execute(f"SELECT STG_TABLE_NAME FROM dbo.TABLE_METADATA WHERE LOGICAL_ID = {logical_id}")
            table_name = crsr.fetchval()

        self.assertEqual(destination_table_name, table_name)

        if delete_record:
            self.delete_metadata_record(destination_target=destination_target, logical_id=logical_id)

    def test_check_table_metadata(self):
        source_target = SqlServerTarget("DEVSQL17TRZ3", "facets", "CMC_CLCL_CLAIM", "dbo")
        destination_target = SqlServerTarget("DEVSQL17TRZRP", "hpXr_Stage", extract_load.create_stage_table_name('CMC_CLCL_CLAIM', 'FACETS'), "stg")

        self.assertEqual(extract_load.check_table_metadata(source_target=source_target, destination_target=destination_target), True)

    def test_update_table_metadata(self):

        destination_target = SqlServerTarget(
            "DEVSQL17TRZRP", 
            "hpXr_Stage", 
            extract_load.create_stage_table_name(process_name=self.process_name, vendor_name=self.vendor_name), 
            "stg", 
            psa_batch_size=-1
        )

        logical_id = extract_load.insert_table_metadata_logical_id(
            process_name=self.process_name, 
            vendor_name=self.vendor_name,
            destination_target=destination_target
        )
        
        extract_load.update_table_metadata(
            process_name=self.process_name, 
            vendor_name=self.vendor_name, 
            destination_target=destination_target
        )

        with pyodbc.connect(destination_target.create_connection_string()) as conn: 
            query = 'SELECT BATCH_SIZE FROM dbo.TABLE_METADATA WHERE LOGICAL_ID = {0}'.format(logical_id)
            crsr = conn.cursor()
            crsr.execute(query)
            batch_size = crsr.fetchval()
            self.assertEqual(batch_size, -1)

        self.delete_metadata_record(destination_target=destination_target, logical_id=logical_id)

    def test_column_metadata_to_json(self):
        columns = [(1, 'MBR_ID_INTERNAL', 'varchar', None, None, None)]

        # source = SqlServerTarget('DEVSQL17TRZ3', 'facets', 'CMC_CLCL_CLAIM', 'dbo')
        # columns = source.get_columns()

        json = extract_load.column_metadata_to_json(columns)
        print(json)

    def test_get_table_metadata_logical_id(self):
        destination_target = SqlServerTarget(
            'DEVSQL17TRZRP',
            'hpXr_Stage',
            extract_load.create_stg_table_name(
                process_name=self.process_name, 
                vendor_name=self.vendor_name
            ),
            'stg',
            chunk_size=None,
            psa_batch_size=None
        )

        record_exists = None
        create_stage_table_name = extract_load.create_stg_table_name(
            process_name=self.process_name,
            vendor_name=self.vendor_name
        )

        create_psa_table_name = extract_load.create_psa_table_name(
            process_name=self.process_name,
            vendor_name=self.vendor_name
        )

        # check if record already exists
        with pyodbc.connect(destination_target.create_connection_string()) as conn:
            crsr = conn.cursor()
            crsr.execute(f"SELECT LOGICAL_ID FROM dbo.TABLE_METADATA WHERE STG_TABLE_NAME = '{create_stage_table_name}' AND PSA_TABLE_NAME = '{create_psa_table_name}'")
            record_exists = crsr.fetchval()
        
        if record_exists == None:
            self.test_insert_table_metadata_logical_id(delete_record=False)

        logical_id = extract_load.get_table_metadata_logical_id(
            process_name=self.process_name,
            vendor_name=self.vendor_name,
            destination_target=destination_target
        )

        table_names = None

        with pyodbc.connect(destination_target.create_connection_string()) as conn:
            crsr = conn.cursor()
            crsr.execute(f"SELECT STG_TABLE_NAME, PSA_TABLE_NAME FROM dbo.TABLE_METADATA WHERE LOGICAL_ID = {logical_id}")
            table_names = crsr.fetchall()

        stage_table_name = table_names[0][0]
        psa_table_name = table_names[0][1]

        self.assertEquals(
           stage_table_name, 
           create_stage_table_name
        )

        self.assertEquals(
            psa_table_name,
            create_psa_table_name
        )

        self.delete_metadata_record(
            destination_target=destination_target, 
            logical_id=logical_id
        )


    def test_pandas_excel(self):
        file = pd.read_excel('c:\\test.xlsx')
        data = {
            'columnd_id' : range(1, len(file.columns) + 1), 
            'column_name' : file.columns, 
            'data_type' : 'varchar', 
            'length' : 0,
            'precision' : 0, 
            'scale' : 0 }
        
        df = pd.DataFrame(data)
        # dtypes = pd.DataFrame(file.dtypes)
        # df = df.join(dtypes, on='column_name', how='inner')
        # df = df.rename(columns={0 : "data_type"})

        # print(df)

        # data_conversion = {
        #     "float64" : "bigint", 
        #     "bytes" : "binary",
        #     "bool" : "bit",
        #     "str" : "varchar",
        #     "datetime" : "datetime",
        #     "int32" : "int",
        #     "object" : "varchar",
        #     "int64" : "bigint"
        # }
        # { 
        #     'python' : [ 'float64', 'bytes', 'bool', 'str', 'datetime', 'int', 'object'], 
        #     'mssql' : ['bigint', 'binery', 'bit', 'varchar', 'datetime', 'int32', 'varchar']
        # }        
        # data_conversion = pd.DataFrame(data_conversion)

        # print(data_conversion['float64'])

        # df['data_type'] = df.apply(lambda x: data_conversion[str(x['data_type'])], axis=1)
        # df['data_type'] = 'varchar'
        # df['length'] = 0
        # df['precision'] = 0
        # df['scale'] = 0

        print(df)

        dic = df.to_json(orient='records')

        process_name = 'CHARTREVIEW'
        vendor_name = 'DYNAMIC'

        destination_table_name = extract_load.create_stg_table_name(process_name=process_name, vendor_name=vendor_name)
        destination_target = SqlServerTarget("DEVSQL17TRZRP", "hpXr_Stage", destination_table_name, "stg")

        extract_load.create_stg_table(
            process_name=process_name,
            vendor_name=vendor_name,
            destination_target=destination_target,
            columns=dic, 
            primary_keys=None
        )


    # test methods    
    def delete_metadata_record(self, destination_target, logical_id):
        with pyodbc.connect(destination_target.create_connection_string()) as conn:
            crsr = conn.cursor()
            crsr.execute(f"DELETE FROM dbo.TABLE_METADATA WHERE LOGICAL_ID = {logical_id}")
            crsr.commit()


if __name__ == '__main__':
    unittest.main()