#!/usr/bin/python
import re
import os
import sys
import subprocess
import time
import configparser
from os import path

class ProspectPostValidation:
    """
    This Class will perform the unit testing after loading data in to prospect database.
    """
    def __init__(self,logger,error,error_log):
        """
        This constructor will enable the logger.
        """
        self.logger = logger
        self.error = error
        self.error_log = error_log

    def validate_record_count_info_file_table(self,prospect_database,file_name,staging_dir,hiveserver2,port,prinicipal):
        """
        This method will load the prospect data in to hadoop.
        """
        self.logger.info("in ProspectPostValidation,validate_record_count_info_file_table method")
        table = file_name.lower()
        info_file = staging_dir+table+"/"+file_name+"---Info.txt"
        self.logger.info("validating the record count in info file and table")
        match_string = "Total Number of Records:" 
        status = os.path.isfile(info_file)
        if (status == True):
            file_read = open(info_file,"r")
            for line in file_read:
                if match_string in line:
                    output = line.split(":")
                    ret = re.search("(\s+)(\S+)",output[1])
                    record_count_info = int((ret.group(2).replace(",","")))
                    self.logger.info("query records found in info file %s"%record_count_info)
        table = table.replace("-","_")
        command = "jdbc:hive2://%s:%s/;principal=%s"%(hiveserver2,port,prinicipal)
        query = "set tez.queue.name=dca;select count(*) from %s.%s"%(prospect_database,table)
        query = "beeline --silent=true --outputformat=csv2 -u \"%s\" -e \"%s\""%(command,query)
        self.logger.info("query executing %s"%query)
        status,output = subprocess.getstatusoutput(query)
        if (status == 0):
            rec_count = output.split("\n")
            table_rec_count = int(rec_count[1])
            self.logger.info("query records found in table %s:%s"%(table,table_rec_count))
        
        if (record_count_info==table_rec_count):
            self.logger.info("record count matched table and info file %s:%s"%(table,info_file))
            records = (table_rec_count*1)/100
            return int(records)
        else:
            self.logger.debug("record count not matched table and info file %s:%s"%(table,info_file))
            self.error.write("record count not matched table and info file %s:%s"%(table,info_file))
            self.logger.debug("record count not matched table and info file %s:%s"%(table,info_file))
            self.error.close()
            time.sleep(1)
            self.send_mail()
            sys.exit(1)
   
    def generate_sample_file_to_aa(self,prospect_database,file_name,scripts_dir,aa_dir,post_dir,records,header,hiveserver2,port,prinicipal):
        """
        This method will load the prospect data in to hadoop.
        """
        self.logger.info("in ProspectPostValication, in generate_sample_file_to_aa method")
        aa_post_files = post_dir+"0*"
        aa_sample_file = scripts_dir+file_name.lower()+"/"+"hadoop_aa.sh"
        self.logger.info("aa sample file:%s"%aa_sample_file)
        fd = open(aa_sample_file,'w')
        table = file_name.lower()
        aa_file = aa_dir+"%s_sample.csv"%table
        table = table.replace("-","_") 
        query = "insert overwrite local directory '%s' row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties(\"seperatorChar\"=\",\",\"quoteChar\"=\"\\\"\") select * from %s.%s distribute by rand() sort by rand() limit %s;"%(post_dir,prospect_database,table,records)
        fd.write("set hive.fetch.task.conversion=none;\n")
        fd.write(query)
        self.logger.info("query:%s"%query)
        fd.close()
        #command = "jdbc:hive2://%s:%s/;principal=%s"%(hiveserver2,port,prinicipal)
        #query_execute = "beeline --silent=true --outputformat=csv2 -u \"%s\" -f %s"%(command,aa_sample_file)
        query_execute = "hive -f %s"%(aa_sample_file)
        print (query_execute)
        status,output = subprocess.getstatusoutput(query_execute)
        if (status == 0):
           self.logger.info("query executed successfully:%s"%query_execute)
           cmd = "cat %s>%s"%(aa_post_files,aa_file)
           status,output = subprocess.getstatusoutput(cmd)
           if (status == 0):
              self.logger.info("sample file generated to post to AA:%s"%aa_file)
              with open(aa_file, 'r+') as f:
                  content = f.read()
                  f.seek(0, 0)
                  f.write(header.rstrip('\r\n') + '\n' + content)
              self.logger.info("adding header to sample file:%s"%aa_file)
              self.send_sample_file_to_AA_via_eai(aa_file)      
           else:
               self.logger.debug("sample file not generated to post to AA:%s"%aa_file)
               self.error.write("FTE100:sample file not generated to post to AA:%s"%aa_file)
               self.logger.debug("FTE100:sample file not generated to post to AA:%s"%aa_file)
               self.error.close()
               time.sleep(1)
               self.send_mail()
               sys.exit(1)
        else:
            self.logger.debug("AA sample file query execution failed:%s"%aa_sample_file)
            self.error.write("FTE100:AA sample file query execution failed:%s"%aa_sample_file)
            self.logger.debug("FTE100:AA sample file query execution failed:%s"%aa_sample_file)
            self.error.close()
            time.sleep(1)
            self.send_mail()
            sys.exit(1)

    def send_sample_file_to_AA_via_eai(self,aa_sample_file):
        """
        This method will send the sample file to AA via EAI.
        """
        string = "EAI_AA"
        self.logger.info("in send_sample_file_to_AA_via_eai method")
        self.logger.info("sending sample file to AA:%s"%aa_sample_file)
        command = "export SB_NETCLASS=PROD;/opt/EAI/bin/eai_send_file -maxfilesize 4000000000 -spath %s -ssys 0BQ.XMISSION -tp AUDIENCE_ACUITY_OBQ -tname \"%s\""%(aa_sample_file,string)
        self.logger.info("executing command:%s"%command)
        """
        status,output = subprocess.getstatusoutput(command)
        if (status == 0):
            self.logger.info("command executed successfully:%s"%command)
            self.logger.info("send sample file to AA:%s"%aa_sample_file)
        else:
            self.logger.debug("sending sample file to AA faild:%s"%aa_sample_file)
            self.error.write("FTE100:sending sample file to AA faild:%s"%aa_sample_file)
            self.logger.debug("FTE100:sending sample file to AA faild:%s"%aa_sample_file)
            self.error.close()
            time.sleep(1)
            self.send_mail()
            sys.exit(1)
        """
    def send_mail(self):
        """
        This method will send a mail.
        """
        self.config=configparser.ConfigParser()
        self.config.read("/data/apps/0bq/ingestion/prospect/conf/prospect.config")
        self.prospect_pdl = self.config.get('PROSPECT','prospect.mail.pdl')

        sendFrom = "prospect_ingestion@sprint.com";
        mail_cmd = "mail -s "+"\"PROSPECT FILE INGESTION DETAILS\"" + " -r" " \"" ;
        mail_cmd = mail_cmd + sendFrom;
        mail_cmd = mail_cmd + "\"";
        mail_cmd = mail_cmd + "  \"";
        mail_cmd = mail_cmd + self.prospect_pdl;
        mail_cmd = mail_cmd + "\"" + " < ";
        mail_cmd = mail_cmd + "%s"%self.error_log;
        status,output = subprocess.getstatusoutput(mail_cmd)
        if (status == 0):
           self.logger.info("Prospect ingestion details,Informed to team")
        else:
           self.logger.debug("sending mail to team failed")
    
    def insert_into_hive_cntl_table(self,database,table,status,file_name,hiveserver2,port,prinicipal):
        """
        This method will insert the record in to cntl table.
        """
        self.logger.info("in insert_into_hive_cntl_table method")
        self.logger.info("insering record in to prospect cntl table:%s"%table)
        command = "jdbc:hive2://%s:%s/;principal=%s"%(hiveserver2,port,prinicipal)
        query = "set tez.queue.name=dca;insert into  table %s.%s values (\'%s\',\'%s\')"%(database,table,file_name.lower(),status)
        query = "beeline --silent=true --outputformat=csv2 -u \"%s\" -e \"%s\""%(command,query)
        self.logger.info("inserting the record in to prospect cntl table:%s"%query)
        self.logger.info("query executing %s"%query)
        status,output = subprocess.getstatusoutput(query)
        if (status == 0):
            self.logger.info("query executed successfully:%s"%query)
        else:
            self.logger.debug("query execution failed,inserting in to hive cntl table:%s"%query)
            self.logger.debug("query execution failed,inserting in to hive cntl table:%s"%query)
            self.error.write("FTE100:query execution failed,inserting in to hive cntl table:%s"%query)
            self.logger.debug("FTE100:query execution failed,inserting in to hive cntl table:%s"%query)
            self.error.close()
            time.sleep(1)
            self.send_mail()
            sys.exit(1)
    
    def backup_consumer_extract(self,file_name,database,hiveserver2,port,prinicipal):
        """
        This method will take the consumer extract backup.
        """
        table = file_name.lower()
        date = time.strftime('%Y%m%d')
        bkp_tbl = table+"_"+str(date)
        self.logger.info("received consumer extract file, we need to take backup")
        self.logger.info("in backup_consumer_extract")
        command = "jdbc:hive2://%s:%s/;principal=%s"%(hiveserver2,port,prinicipal)
        query = "set tez.queue.name=dca;alter table %s.%s rename to %s.%s"%(database,table,database,bkp_tbl)
        query = "beeline --silent=true --outputformat=csv2 -u \"%s\" -e \"%s\""%(command,query)
        self.logger.info("query executing %s"%query)
        status,output = subprocess.getstatusoutput(query)
        if (status == 0):
            self.logger.info("query executed successfully:%s"%query)
            self.logger.info("created backup table %s:%s"%(database,bkp_tbl))
        else:
            self.logger.debug("query execution failed,creating backup table for consumer extract:%s"%query)
            self.error.write("FTE100:query execution failed,creating backup table for consumer extract:%s"%query)
            self.logger.debug("FTE100:query execution failed,creating backup table for consumer extract:%s"%query)
            self.error.close()
            time.sleep(1)
            self.send_mail()
    
    def file_check(self,database,table,file,hiveserver2,port,prinicipal):
        """
        This method will insert the record in to cntl table.
        """
        self.logger.info("in duplicate file check method,validating file:%s"%file)
        column_name = "file"
        command = "jdbc:hive2://%s:%s/;principal=%s"%(hiveserver2,port,prinicipal)
        query = "set tez.queue.name=dca;select count(*) from %s.%s where %s=\'%s\'"%(database,table,column_name,str(file.lower()))
        execute_query = "beeline --silent=true --outputformat=csv2 -u \"%s\" -e \"%s\""%(command,query)
        self.logger.info("inserting the record in to prospect cntl table:%s"%execute_query)
        self.logger.info("query executing %s"%execute_query)
        status,output = subprocess.getstatusoutput(execute_query)
        if (status == 0):
            self.logger.info("query executed successful:%s"%execute_query)
            data = output.split("\n")
            count = int(data[1])
            if (count >= 1):
                 self.logger.debug("Received duplicate file :%s"%file)
                 self.error.write("FTE100:received duplicate file:%s"%file)
                 self.logger.debug("FTE100:received duplicate file:%s"%file)
                 self.logger.debug("Received duplicate file:%s"%count)
                 self.error.close()
                 time.sleep(1)
                 self.send_mail()
                 sys.exit(1)
            else:
                 self.logger.info("Received non duplicate file, calling pre-validation:%s"%file)
                 self.logger.info("Received non duplicate file, calling pre-validation:%s"%count)
        else:
            self.logger.debug("Query failed to execute,Duplicate file check:%s"%file)
            self.error.write("FTE100:Query execution failed, Duplicate file check:%s"%file)
            self.logger.debug("FTE100:Query execution failed, Duplicate file check:%s"%file)
            self.error.close()
            time.sleep(1)
            self.send_mail()
            sys.exit(1)

    def rename_staging_scripts_hdfs(self,file_name,prospect_scripts_dir,prospect_staging_dir,prospect_hdfs_dir):
        """
        This method will insert the record in to cntl table.
        """
        file_name = file_name.lower()
        self.logger.info("in rename_staging_scripts_hdfs:%s"%file_name)
        date = time.strftime('%Y%m%d')
        source_scripts_dir = prospect_scripts_dir+file_name
        self.logger.info("source scripts directory:%s"%source_scripts_dir)
        source_scripts_dir_bkp = prospect_scripts_dir+file_name+"_"+date
        self.logger.info("source backup scripts directory:%s"%source_scripts_dir_bkp)
        source_staging_dir = prospect_staging_dir+file_name
        self.logger.info("source staging directory:%s"%source_staging_dir)
        source_staging_dir_bkp = prospect_staging_dir+file_name+"_"+date
        self.logger.info("source backup staging directory:%s"%source_staging_dir_bkp)
        source_hdfs_dir = prospect_hdfs_dir+file_name
        self.logger.info("source hdfs directory:%s"%source_hdfs_dir)
        source_hdfs_dir_bkp = prospect_hdfs_dir+file_name+"_"+date
        self.logger.info("source backup hdfs directory:%s"%source_hdfs_dir_bkp)
        hdfs_cmd = "hdfs dfs -ls %s"%source_hdfs_dir
        hdfs_cmd_bkp = "hdfs dfs -mv %s %s"%(source_hdfs_dir,source_hdfs_dir_bkp)

        #changing the scripts directory 
        if path.exists(source_scripts_dir):
            self.logger.info("creating backup dir directory:%s to backup_dir:%s"%(source_scripts_dir,source_scripts_dir_bkp))
            os.rename(source_scripts_dir,source_scripts_dir_bkp) 
            self.logger.info("created backup dir directory:%s to backup_dir:%s"%(source_scripts_dir,source_scripts_dir_bkp))
        if path.exists(source_staging_dir):
            self.logger.info("creating backup dir directory:%s to backup_dir:%s"%(source_staging_dir,source_staging_dir_bkp))
            os.rename(source_staging_dir,source_staging_dir_bkp) 
            self.logger.info("created backup dir directory:%s to backup_dir:%s"%(source_staging_dir,source_staging_dir_bkp)) 
        status,output = subprocess.getstatusoutput(hdfs_cmd)
        if (status == 0):
            self.logger.info("hdfs directory found:%s"%source_hdfs_dir)
            status,output = subprocess.getstatusoutput(hdfs_cmd_bkp)
            if (status == 0): 
                self.logger.info("created backup dir directory:%s to backup_dir:%s"%(source_hdfs_dir,source_hdfs_dir_bkp))
            else:
                self.logger.debug("not created backup dir directory:%s to backup_dir:%s"%(source_hdfs_dir,source_hdfs_dir_bkp))
                self.error.write("FTE100:not created bkup dir directory:%s to backup_dir:%s"%(source_hdfs_dir,source_hdfs_dir_bkp))
                self.error.debug("FTE100:not created bkup dir directory:%s to backup_dir:%s"%(source_hdfs_dir,source_hdfs_dir_bkp))
                self.error.close()
                time.sleep(1)
                self.send_mail()
                sys.exit(1)
        else: 
            self.logger.debug("Not found hdfs dir:%s"%source_hdfs_dir)
