from pymongo import MongoClient
from logs import logs_config


client = MongoClient("mongodb://13.127.57.185:27017/",compressors='zstd',zlibCompressionLevel=9)
logs_config.logger.info("Database connection successful")
def get_database():
    try:
        database = client.pvvnl 
        return database
    except Exception as e:
        logs_config.logger.error("Database connection failed:", exc_info=True)
        raise e
        

