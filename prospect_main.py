#!/usr/bin/python
import re
import logging
import configparser
from prospect_pre_validation import ProspectPreValidation
from prospect_hadoop_processing import ProspectHadoopProcessing
from prospect_postvalidation import ProspectPostValidation
import time

class ProspectMain:
    """
    This Class will be starting point for the prospect application.
    """
    def __init__(self):
        """
        This constructor will read the conf from prospect conf file
        """
        self.config=configparser.ConfigParser()
        self.config.read("/data/apps/0bq/ingestion/prospect/conf/prospect.config")
        self.prospect_ingestion_dir = self.config.get('PROSPECT','prospect.ingestion.dir')
        self.prospect_staging_dir = self.config.get('PROSPECT','prospect.staging.dir')
        self.prospect_log_dir = self.config.get('PROSPECT','prospect.log.dir')
        self.prospect_scripts_dir = self.config.get('PROSPECT','prospect.scripts.dir')
        self.prospect_hdfs_dir = self.config.get('PROSPECT','prospect.hdfs.dir')
        self.prospect_aa_dir = self.config.get('PROSPECT','prospect.aa.dir')
        self.prospect_postvalidation_dir = self.config.get('PROSPECT','prospect.postvalidation.dir')
        
        #PROSPECT HADOOP CONFIGURATION
        self.hive_external_database = self.config.get('HADOOP','prospect.external.database')
        self.hive_database = self.config.get('HADOOP','prospect.database')
        self.prospect_cntl_database = self.config.get('HADOOP','prospect.cntl.database')
        self.prospect_cntl_table = self.config.get('HADOOP','prospect.cntl.table')
        self.prospect_hiveserver = self.config.get('HADOOP','prospect.hive.server2')
        self.prospect_hive_port = self.config.get('HADOOP','prospect.hive.port')
        self.prospect_hive_prinicipal = self.config.get('HADOOP','prospect.hive.prinicipal')

        self.log_error_name = str(self.prospect_log_dir)+"error"+".log"
        self.error_log = open(self.log_error_name,"w+")

        self.logger = logging.getLogger("PROSPECT")
        self.logger.setLevel(logging.DEBUG)
        self.log_file_name = str(self.prospect_log_dir)+"prospect"+"_"+"debug"+".log"
        self.fh = logging.FileHandler(self.log_file_name)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fh.setFormatter(self.formatter)
        self.logger.addHandler(self.fh)
        self.prospectPreValidation = ProspectPreValidation(self.logger,self.error_log,self.log_error_name)
        self.prospectPostValidation = ProspectPostValidation(self.logger,self.error_log,self.log_error_name)
        self.prospectHadoopProcessing = ProspectHadoopProcessing(self.logger,self.error_log,self.log_error_name)

    def prospect_start(self):
        """
        starting point of the prospect application, which will monitor the prospect_ingestion directory for info file.
        """
        
        #check for the success file in ingestion directory.
        self.prospectPreValidation.monitor_ingestion_dir_for_success_file(self.prospect_ingestion_dir,self.prospect_aa_dir,self.prospectPostValidation)
        
        #check for the failure file in ingestion directory.
        self.prospectPreValidation.monitor_ingestion_dir_for_failure_file(self.prospect_ingestion_dir,self.prospect_aa_dir,self.hive_database,self.prospectPostValidation,self.prospect_hiveserver,self.prospect_hive_port,self.prospect_hive_prinicipal)

         
        #check for the info in ingestion directory
        info_ret =self.prospectPreValidation.monitor_ingestion_dir_for_info_file(self.prospect_ingestion_dir,self.prospectPostValidation)   
        
        if (info_ret == True):             
             self.logger.info("Prospect info file found ingestion dir %s"%self.prospect_ingestion_dir)
             self.logger.info("Calling get_prospect_file_name method")
             file_name = self.prospectPreValidation.get_prospect_file_name(self.prospect_ingestion_dir,self.prospectPostValidation)
        
             if (file_name.lower() == "consumer_extract"):
                  self.prospectPostValidation.backup_consumer_extract(file_name,self.hive_database)
                  self.prospectPostValidation.rename_staging_scripts_hdfs(file_name,self.prospect_scripts_dir,self.prospect_staging_dir,self.prospect_hdfs_dir)
             else:  
                  self.prospectPostValidation.file_check(self.prospect_cntl_database,self.prospect_cntl_table,file_name,self.prospect_hiveserver,self.prospect_hive_port,self.prospect_hive_prinicipal)
             
              
             self.logger.info("Prospect file received:%s"%file_name)
             self.logger.info("Calling rename_prospect_info_file method as info file name has spaces")
             
             renamed_info_file = self.prospectPreValidation.rename_prospect_info_file(self.prospect_ingestion_dir,self.prospectPostValidation)
             self.logger.info("renamed Info Prospect file:%s"%renamed_info_file)
             self.logger.info("callind pre_validation method ")
             
             self.prospectPreValidation.pre_validation(renamed_info_file,file_name,self.prospect_ingestion_dir,self.prospect_staging_dir,self.prospectPostValidation)
             self.logger.info("pre_validation completed successfully ")
                         
                 
             #Hadoop Processing
             self.logger.info("starting hadoop processing ") 
             self.logger.info("calling prospect_hadoop_processing method ") 
             header = self.prospectHadoopProcessing.prospect_hadoop_processing(self.prospect_staging_dir,file_name,self.prospect_hdfs_dir,self.hive_external_database,self.hive_database,self.prospect_scripts_dir,self.prospectPostValidation,self.prospect_hiveserver,self.prospect_hive_port,self.prospect_hive_prinicipal)
             self.logger.info("Hadoop processing completed successfully ")
             
             self.logger.info("Prospect post validation started")  
             self.logger.info("calling validate_record_count_info_file_table method")
         
             records = self.prospectPostValidation.validate_record_count_info_file_table(self.hive_database,file_name,self.prospect_staging_dir,self.prospect_hiveserver,self.prospect_hive_port,self.prospect_hive_prinicipal)
             self.logger.info("returned 5 percent of records:%s"%records) 
    
             self.logger.info("calling generate_sample_file method")
             self.prospectPostValidation.generate_sample_file_to_aa(self.hive_database,file_name,self.prospect_scripts_dir,self.prospect_aa_dir,self.prospect_postvalidation_dir,records,header,self.prospect_hiveserver,self.prospect_hive_port,self.prospect_hive_prinicipal)
             self.prospectPostValidation.insert_into_hive_cntl_table(self.prospect_cntl_database,self.prospect_cntl_table,"loaded",file_name,self.prospect_hiveserver,self.prospect_hive_port,self.prospect_hive_prinicipal)   
                     
        #else:
        #    time.sleep(300)
        #    self.prospect_start()
   
if __name__ == '__main__':
     """
     Starting point of the project.
     """
     prospectMain = ProspectMain()
     prospectMain.prospect_start()
