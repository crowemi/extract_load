import pandas as pd 
import logging 

from .base_file_target import BaseFileTarget

class ExcelTarget(BaseFileTarget): 
    def __init__(self, configuration):
        super().__init__(configuration)

