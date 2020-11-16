import logging

class BaseFileTarget(): 
    def __init__(self, configuration):
        
        self.vendor_name = None if not 'vendor_name' in configuration else configuration['vendor_name'] #required
        self.process_name = None if not 'process_name' in configuration else configuration['process_name'] #required
        self.path = None if not 'path' in configuration else configuration['path'] #required

        if not self.vendor_name:
            raise ValueError("The 'vendor_name' attribute is required and was not supplied.")
        if not self.process_name:
            raise ValueError("The 'process_name' attribute is required and was not supplied.")
        if not self.path: 
            raise ValueError("The 'path' attribute is required and was not supplied")

        self.psa_batch_size = None if not 'psa_batch_size' in configuration else configuration['psa_batch_size'] #optional
        self.is_archive_file = None if not 'archive_file' in configuration else configuration['archive_file'] #optional
        self.archive_file_path = None if not 'archive_file_path' in configuration else configuration['archive_file_path'] #required

        if self.is_archive_file and not self.archive_file_path: 
            raise ValueError("The archive_file flag was set but no archive file path was specified.")

        self.is_delete_file = None if not 'delete_file' in configuration else configuration['delete_file'] #optional
        self.first_row_header = None if not 'first_row_header' in configuration else configuration['first_row_header'] #optional
        self.skip_rows = 0 if not 'skip_rows' in configuration or not configuration['skip_rows'] else configuration['skip_rows'] #optional
        self.primary_key = None if not 'primary_key' in configuration else configuration['primary_key'] #optional

    def get_is_delete_file(self):
        return self.is_delete_file
    def set_is_delete_file(self, is_delete_file):
        self.is_delete_file = is_delete_file
    
    def get_is_archive_file(self):
        return self.is_archive_file
    def set_is_archive_file(self, is_archive_file):
        self.is_archive_file = is_archive_file

    def get_archive_file_path(self):
        return self.archive_file_path
    def set_archive_file_path(self, archive_file_path):
        self.archive_file_path = archive_file_path
    
    def get_primary_key(self):
        return self.primary_key
    def set_primary_key(self, primary_key):
        self.primary_key = primary_key

    def get_skip_rows(self):
        return self.skip_rows
    def set_skip_rows(self, skip_rows):
        self.skip_rows

    def get_path(self):
        return self.path

    def set_path(self, path):
        self.path = path

    def get_process_name(self):
        return self.process_name

    def set_process_name(self, process_name):
        self.process_name = process_name

    def get_vendor_name(self): 
        return self.vendor_name

    def set_vendor_name(self, vendor_name):
        self.vendor_name = vendor_name

    def get_psa_batch_size(self):
        return self.psa_batch_size

    def set_psa_batch_size(self, psa_batch_size):
        self.psa_batch_size = psa_batch_size