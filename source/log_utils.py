import logging
import logging.handlers
import boto3
from typing import Optional

def setup_logger(log_level: str, s3_bucket: Optional[str] = None, log_file: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.getLevelName(log_level))
    
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if s3_bucket:
        s3_client = boto3.client('s3')
        s3_handler = logging.handlers.S3Handler(s3_client, s3_bucket)
        s3_handler.setFormatter(formatter)
        logger.addHandler(s3_handler)
    elif log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
