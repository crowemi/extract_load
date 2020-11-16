import pandas as pd 
import logging 

from .base_file_target import BaseFileTarget 

class CsvTarget(BaseFileTarget): 
    def __init__(self, configuration): 
        super().__init__(configuration)

        self.delimiter = None if 'delimiter' not in configuration else configuration['delimiter']
        