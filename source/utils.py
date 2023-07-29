import logging
import logging.handlers
import toml
import boto3

def setup_logger(log_level, s3_bucket=None, log_file=None):
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

def load_config(path, mode):
    try:
        with open(path, mode) as file:
            config = toml.load(file)
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error("An error occurred in load_config method..", exc_info=True)
    else:
        return config

config_path = '/home/ubuntu/aws_restbus_proj/source/config.toml'


# Function to read Hudi options from the TOML file
def read_hudi_options(file_path):
    try:
        with open(file_path, "r") as file:
            hudi_options = toml.load(file)["hoodie_options"]
        return hudi_options
    except Exception as e:
        # Handle the error if the file cannot be read or parsed
        print("An error occurred while reading the TOML file:", e)
        return None

# # Read the Hudi options from the TOML file
# hudi_options = read_hudi_options(config_path)