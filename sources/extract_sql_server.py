import urllib
from sqlalchemy import create_engine
import pyodbc
import logging

class SqlServerTarget:
    
    def __init__(self, server, database, table, schema, chunk_size=None, psa_batch_size=None):
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
        self._chunk_size = chunk_size
        self._table_metadata_logical_id = None
        self._psa_batch_size = psa_batch_size

    def create_connection_string(self):
        # 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+'; Trusted_Connection=yes'
        return 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self._server + ';DATABASE=' + self._database + '; Trusted_Connection=yes'

    #TODO: Create unit test
    def get_server_name(self):
        return self._server

    #TODO: Create unit test
    def get_database_name(self):
        return self._database

    #TODO: Create unit test
    def get_table_name(self):
        return self._table

    #TODO: Create unit test
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

    #TODO: configurable isolation_level
    def create_engine(self, isolation_level=None):
        connection_string = self.create_connection_string()
        params = urllib.parse.quote_plus(connection_string)
        # isolation_level="SNAPSHOT
        return create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True, isolation_level="READ COMMITTED" if isolation_level is None else isolation_level)
   
    def get_primary_keys(self): 
        """Get primary key metadata (column_id, column_name, data_type, length, precision, scale). Return list of columns."""
        with pyodbc.connect(self.create_connection_string()) as conn:
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
                            and s.name = '{self.get_schema_name()}'
                    where t.name = '{self.get_table_name()}'
                            )
                    and i.is_primary_key = 1
                order by column_id asc
            """
            crsr = conn.cursor()
            crsr.execute(query)
            return crsr.fetchall()

    def get_columns(self):
        """Get column metadata (column_id, column_name, data_type, length, precision, scale). Return list of columns."""
        with pyodbc.connect(self.create_connection_string()) as conn: 
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
                    join sys.schemas s on s.schema_id = ta.schema_id 
                        and s.name = '{self.get_schema_name()}'
                where ta.name = '{self.get_table_name()}'
                order by column_id asc
            """
            crsr = conn.cursor()
            crsr.execute(query)
            return crsr.fetchall()

    def check_change_tracking (self):
        """Check that change tracking is enabled on object table. Return boolean."""
        check_change_tracking_query = f"SELECT COUNT(1) FROM {self.get_database_name()}.sys.change_tracking_tables ctt JOIN {self.get_database_name()}.sys.tables t ON t.object_id = ctt.object_id AND t.name = '{self.get_table_name()}'"
        with pyodbc.connect(self.create_connection_string()) as conn:
            cursor = conn.cursor()
            cursor.execute(check_change_tracking_query)
            return bool(cursor.fetchval())

    def check_table_exists(self):
        """Check that object table exists. Return bool."""
        with pyodbc.connect(self.create_connection_string()) as conn:
            crsr = conn.cursor()
            crsr.execute(f"SELECT COUNT(1) FROM {self.get_database_name()}.sys.tables t WHERE t.name = '{self.get_table_name()}'")
            return bool(crsr.fetchval())

    def add_change_tracking (self):
        """Add change tracking to object table."""

        add_change_tracking_query = f"ALTER TABLE {self.get_database_name()}.{self.get_schema_name()}.{self.get_table_name()} ENABLE CHANGE_TRACKING"
        with pyodbc.connect(self.create_connection_string()) as conn:
            cursor = conn.cursor()
            cursor.execute(add_change_tracking_query)

    def get_new_change_tracking_key(self):
        """Get new change tracking key for object database. Return change tracking key (integer)."""

        logger = logging.getLogger()
        logger.debug('get_new_change_tracking_key: get_new_change_tracking_key Entering.')
        
        get_new_change_key_query = f"SELECT CHANGE_TRACKING_CURRENT_VERSION()"
        
        with pyodbc.connect(self.create_connection_string()) as conn:
            crsr = conn.cursor()
            ret = crsr.execute(get_new_change_key_query).fetchval()

        logger.info('get_new_change_tracking_key: new change key {0}.'.format(ret))
        logger.debug('get_new_change_tracking_key: get_new_change_tracking_key Leaving.')
        
        return ret

    def get_psa_batch_size(self):
        return self._psa_batch_size