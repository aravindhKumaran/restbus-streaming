import logging
import logging.handlers
import boto3
import toml
from typing import Optional, Dict, Any

class LogClass:
    format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def __init__(self, module_name):
        self.logger = logging.getLogger(module_name)
        self.logger.setLevel(logging.DEBUG)

    def console_handler(self):
        """creates a console handler and adds to logger"""
        c_handler = logging.StreamHandler()
        c_handler.setLevel(logging.DEBUG)
        c_handler.setFormatter(self.format)
        self.logger.addHandler(c_handler)

    def file_handler(self, filename, writemode):
        """creates a file handler and adds to logger"""
        f_handler = logging.FileHandler(filename, mode=writemode)
        f_handler.setLevel(logging.INFO)
        f_handler.setFormatter(self.format)
        self.logger.addHandler(f_handler)

    def s3_bucket_handler(self, s3_bucket):
        """creates a s3_bucket handler and adds to logger"""
        s3_client = boto3.client('s3')
        s3_handler = logging.handlers.S3Handler(s3_client, s3_bucket)
        s3_handler.setLevel(logging.ERROR)
        s3_handler.setFormatter(self.format)
        self.logger.addHandler(s3_handler)   

class UtilsClass:
    
    def __init__(self, module_name):  
        self.config_path = '/home/ubuntu/restbus-streaming/sourceFile/properties/config.toml'
        self.config = self.load_config(self.config_path, 'r')
        self.envn = self.config['spark']['envn']
        self.s3_logs = self.config['prod']['logs_dir']
        self.module_name = module_name
        
        if self.envn == "dev":
            self.logger = self.create_dev_logger("app.log", mode='a')
        elif self.envn == "prod":
            self.logger = self.create_prod_logger(self.s3_logs)
        

    def create_dev_logger(self, filePath: str, mode: str ='a'):
        try:
            log_obj = LogClass(self.module_name)
            log_obj.console_handler()
            log_obj.file_handler(filePath, mode)
            logger = log_obj.logger
            return logger
        except Exception as e:
            raise e

    def create_prod_logger(self, s3_bucket: str):
        try:
            log_obj = LogClass(self.module_name)
            log_obj.console_handler()
            log_obj.s3_bucket_handler(s3_bucket)
            logger = log_obj.logger
            return logger
        except Exception as e:
            raise e

    def load_config(self, path: str, mode: str) -> Dict[str, Any]:
        try:
            with open(path, mode) as file:
                config = toml.load(file)
        except Exception as e:
            self.logger.error("An error occurred in load_config method..", exc_info=True)
            raise e
        else:
            return config

    def read_hudi_options(self, file_path: str) -> Dict[str, Any]:
        try:
            with open(file_path, "r") as file:
                hudi_options = toml.load(file)["hoodie_options"]
            return hudi_options
        except Exception as e:
            # Handle the error if the file cannot be read or parsed
            self.logger.error("An error occurred while reading the TOML file:", exc_info=True)
            raise e
