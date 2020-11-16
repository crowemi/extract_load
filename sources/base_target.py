import logging 
import pyodbc
import sys

class BaseTarget(): 
    def __init__(self, process_name, vendor_name):
        self.process_name = process_name
        self.vendor_name = vendor_name
        self.logger = logging.getLogger()
           
    def get_stg_table_name(self): 
        """Create a stage table name for the given process and vendor name.

            Keyword arguments:
            process_name - The process name for this stage table (i.e. source table name). (string) 
            vendor_name - The vendor name for this stage table (i.e. source database name). (string)
        """
        return 'STG_{0}_{1}'.format(self.vendor_name, self.process_name)
    
    def get_psa_table_name(self): 
        """Create a persistant stage table name for the given process and vendor name.

            Keyword arguments:
            process_name - The process name for this stage table (i.e. source table name). (string) 
            vendor_name - The vendor name for this stage table (i.e. source database name). (string)
        """
        return 'PSA_{0}_{1}'.format(self.vendor_name, self.process_name)