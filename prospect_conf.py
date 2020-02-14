#!/usr/bin/python
import re
import configparser

class ProspectConf:
    """
    This Class will be reading the configuration from prospect conf file.
    """
    def __init__(self,logger):
        """
        This constructor will read the conf from prospect conf file
        """
        self.logger = logger
        self.logger.info("in prospect_conf,ProspectConf module")
        self.logger.info("Below is the given configuration")
        config=configparser.ConfigParser()
        config.read("/data/apps/0bq/ingestion/prospect/conf/prospect.config")
        self.prospect_ingestion_dir = config.get('PROSPECT','prospect.ingestion.dir')
        self.prospect_staging_dir = config.get('PROSPECT','prospect.staging.dir')
        self.prospect_log_dir = config.get('PROSPECT','prospect.log.dir')
        self.prospect_hdfs_dir = config.get('PROSPECT','prospect.hdfs.dir')
        #PROSPECT HADOOP CONFIGURATION
        self.hive_server2 = config.get('HADOOP','prospect.hive.server2')
        self.hive_port = config.get('HADOOP','prospect.port')
        self.hive_prinicipal = config.get('HADOOP','prospect.prinicipal')
        self.hive_database = config.get('HADOOP','prospect.database')
