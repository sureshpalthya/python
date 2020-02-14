#!/usr/bin/python
import re
import os
import sys
import shutil
import subprocess
import csv
import time

class ProspectHadoopProcessing:
    """
    This Class will process and load the process data in to Hadoop.
    """
    def __init__(self,logger,error_log,error_mail):
        """
        This constructor will enable the logger.
        """
        self.logger = logger
        self.error = error_log
        self.error_log = error_mail

    def prospect_hadoop_processing(self,prospect_staging_dir,file_name,hdfs_dir,external_db,prospect_db,scripts_dir,post_validation,hiveserver2,port,prinicipal):
        """
        This method will load the prospect data in to hadoop.
        """
        self.logger.info("in ProspectHadoopProcessing class,prospect_hadoop_processing method ")
        prospect_file = file_name+"."+"csv"
        staging_dir = prospect_staging_dir+file_name.lower()
        csv_file = os.path.join(staging_dir,prospect_file)
        self.logger.info("csv_file, we need to process:%s"%csv_file)
        status = os.path.isfile(csv_file)
        if (status == True): 
            self.logger.info("csv_file, exists:%s"%csv_file)
            self.logger.info("csv_file, exists:%s,calling read_csv_file_prepare_header method"%csv_file)
            table_header,header = self.read_csv_file_prepare_header(csv_file,post_validation)
            self.logger.info("Got the table hearder")
            self.logger.info("calling creating external table method")
            
            external_ddl_script = self.create_external_table(table_header,file_name,hdfs_dir,external_db,scripts_dir) 
            self.logger.info("calling creating internal table method")
            internal_ddl_script = self.create_internal_table(table_header,file_name,prospect_db,external_db,scripts_dir)
            self.logger.info("calling copy_csv_to_hdfs method")
            
            self.copy_csv_to_hdfs(file_name,csv_file,hdfs_dir,post_validation)
            self.logger.info("calling execute_hive_scripts to execute external table script")
            self.execute_hive_scripts(external_ddl_script,post_validation,hiveserver2,port,prinicipal)
            self.logger.info("calling execute_hive_scripts to execute internal table script")
            self.execute_hive_scripts(internal_ddl_script,post_validation,hiveserver2,port,prinicipal)
            self.logger.info("hadoop processing completed successfully")
            return header
        else:
            self.logger.debug("csv file not found in staging directory:%s"%csv_file)
            self.error.write("FTE001:csv file not found in staging directory:%s"%csv_file)
            self.logger.debug("FTE001:csv file not found in staging directory:%s"%csv_file)
            self.error.close()
            time.sleep(1)
            post_validation.send_mail()
            sys.exit(1)

    def read_csv_file_prepare_header(self,prospect_csv_file,post_validation):
        """
        This method will read the csv file and return the table header.
        """
        self.logger.info("in ProspectHadoopProcessing class,read_csv_file_prepare_header method ")
        table_header=[]
        fd = open(prospect_csv_file,'r')
        header = fd.readline()
        data=csv.reader(header, quotechar='"', delimiter=',',quoting=csv.QUOTE_ALL)
        for attr in data:
            if (len(attr)==1):
                attribute = str(attr[0]).strip('[]')
                attr = attribute.replace(' ','_').replace('-','_').replace(',','_')
                table_header.append(attr)
        if (len(table_header) != 0):
            self.logger.info("header is not empty, length of fileds:%s"%(len(table_header)))
            return table_header,header
        else:
            self.logger.debug("header is empty, length of fileds:%s"%(len(table_header)))
            self.error.write("FTE001:header is empty, length of fileds:%s"%(len(table_header)))
            self.logger.debug("FTE001:header is empty, length of fileds:%s"%(len(table_header)))
            self.error.close()
            time.sleep(1)
            post_validation.send_mail()
            sys.exit(1)
            return False
   
    def create_external_table(self,table_header,file_name,hdfs_path,external_db,scripts_dir):
        """
        This method will create the external table ddl.
        """
        self.logger.info("in ProspectHadoopProcessing class, create_external_table method")
        file_name=file_name.lower()
        hdfs_location = hdfs_path+file_name
        self.logger.info("hdfs location:%s"%hdfs_location)
        scripts_dir = scripts_dir+file_name+"/"
        self.logger.info("scripts_dir:%s"%scripts_dir)
        os.makedirs(scripts_dir)
        external_file_dir = scripts_dir+file_name+"_"+"external.sh"
        self.logger.info("external_file_dir:%s"%external_file_dir)
        self.logger.info("generating external table DDL in external_table_ddl_file_dir:%s"%external_file_dir)
        file_name=file_name.replace("-","_")
        fd = open(external_file_dir,"w")
        fd.write("set hive.execution.engine=MR;\n")
        fd.write("CREATE EXTERNAL TABLE %s.%s ( \n"%(external_db,file_name))
        for attr in table_header:
            if (attr==table_header[-1]):
                field = attr+" "+"String"+")"
            else:
                field = attr+" "+"String"+","
            fd.write(field+'\n')
        fd.write("ROW FORMAT SERDE \n")
        fd.write("   'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n")
        fd.write("WITH SERDEPROPERTIES (\n")
        fd.write("    'quoteChar'='\\\"',\n")
        fd.write("    'separatorChar'=',')\n")
        fd.write("STORED AS TEXTFILE\n")
        fd.write("LOCATION '%s'\n"%hdfs_location)
        fd.write("tblproperties (\"skip.header.line.count\"=\"1\");")
        fd.write("\n")
        fd.write("analyze table %s.%s compute statistics;"%(external_db,file_name))
        fd.close()
        self.logger.info("external_file_dir:%s has been created successfully"%external_file_dir)
        return external_file_dir
    def create_internal_table(self,table_header,file_name,prospect_db,external_db,scripts_dir):
        """
        This method will create internal table ddl.
        """
        self.logger.info("in ProspectHadoopProcessing class, create_internal_table method")
        file_name=file_name.lower()
        internal_file_dir = scripts_dir+file_name+"/"+file_name+"_internal.sh"
        self.logger.info("internal_table_ddl_file_dir:%s"%internal_file_dir)
        self.logger.info("generating internal table DDL in internal_table_ddl_file_dir:%s"%internal_file_dir)
        fd = open(internal_file_dir,"w")
        file_name=file_name.replace("-","_")
        fd.write("set hive.execution.engine=MR;\n")
        fd.write("CREATE TABLE %s.%s ( \n"%(prospect_db,file_name))
        counter = 0
        if (re.search('id',str(table_header[0]),flags=re.IGNORECASE) != None):
            for attr in table_header:
                if (re.search('id',str(table_header[0]),flags=re.IGNORECASE) != None) and (counter == 0):
                    field = attr+" "+"bigint"+","
                    counter = counter+1
                elif (attr==table_header[-1]):
                    field = attr+" "+"String"+")"
                else:
                    field = attr+" "+"String"+","
                fd.write(field+'\n')
            fd.write("CLUSTERED BY (\n")
            fd.write("    %s)\n"%table_header[0])
            fd.write("SORTED BY (\n")
            fd.write("    %s)\n"%table_header[0])
            fd.write("INTO 25 BUCKETS\n")
            fd.write("STORED AS ORC\n")
            fd.write("TBLPROPERTIES (\n")
            fd.write("    'orc.compress'='SNAPPY',\n")
            fd.write("    'orc.compress.size'='262144',\n")
            fd.write("    'orc.create.index'='true',\n")
            fd.write("    'orc.bloom.filter.columns'='%s',\n"%table_header[0])
            fd.write("    'orc.bloom.filter.fpp'='0.05',\n")
            fd.write("    'orc.row.index.stride'='10000',\n")
            fd.write("    'orc.stripe.size'='268435456');")
            fd.write("\n")
            fd.write("insert into %s.%s select * from %s.%s;"%(prospect_db,file_name,external_db,file_name))
            fd.close()
            self.logger.info("generated internal table DDL in %s"%internal_file_dir)
            return internal_file_dir
        else:
            for attr in table_header:
                if (attr==table_header[-1]):
                    field = attr+" "+"String"+")"
                else:
                    field = attr+" "+"String"+","
                fd.write(field+'\n')
            fd.write("STORED AS ORC;\n")
            fd.write("\n")
            fd.write("insert into %s.%s select * from %s.%s;"%(prospect_db,file_name,external_db,file_name))
            fd.close()
            self.logger.info("generated internal table DDL in %s"%internal_file_dir)
            return internal_file_dir

    def copy_csv_to_hdfs(self,filename,csv_path,hdfs_location,post_validation):
        """
        This method is will copy the data from local dir to hdfs
        """
        self.logger.info("in ProspectHadoopProcessing class,in copy_csv_to_hdfs method")
        hdfs_path = str(hdfs_location)
        hdfs_dir = hdfs_path+filename.lower()+"/"
        self.logger.info("hdfs_directory:%s"%hdfs_dir)
        cmd = "hadoop dfs -mkdir %s"%(hdfs_dir)
        self.logger.info("creating hdfs_directory:%s"%cmd)
        copy_cmd = "hadoop dfs -copyFromLocal %s %s"%(csv_path,hdfs_dir)
        status,output = subprocess.getstatusoutput(cmd)
        if (status == 0):
            self.logger.info("created hdfs_directory:%s"%cmd)
            self.logger.info("copying csv file:%s to hdfs_directory:%s"%(csv_path,hdfs_dir))
            status,output = subprocess.getstatusoutput(copy_cmd)
            if (status == 0):
                self.logger.info("copied csv file:%s to hdfs_directory:%s successfully"%(csv_path,hdfs_dir))
            else:
                self.logger.debug("copy failed csv file:%s to hdfs_directory:%s"%(csv_path,hdfs_dir))
                self.error.write("FLE001:copy failed csv file:%s to hdfs_directory:%s"%(csv_path,hdfs_dir))
                self.logger.debug("FLE001:copy failed csv file:%s to hdfs_directory:%s"%(csv_path,hdfs_dir))
                self.error.close()
                time.sleep(1)
                post_validation.send_mail()
                sys.exit(1)
        else:
            self.logger.debug("Failed to create hdfs_directory:%s"%hdfs_dir)
            self.error.write("FLE001:Failed to create hdfs_directory:%s"%hdfs_dir)
            self.logger.debug("FLE001:Failed to create hdfs_directory:%s"%hdfs_dir)
            self.error.close()
            time.sleep(1)
            post_validation.send_mail()
            sys.exit(1)
       
    def execute_hive_scripts(self,script_path,post_validation,hiveserver2,port,prinicipal):
        """
        This method is will copy the data from local dir to hdfs
        """
        self.logger.info("in ProspectHadoopProcessing class,in execute_hive_scripts")
        self.logger.info("executing script:%s"%script_path)
        command = "jdbc:hive2://%s:%s/;principal=%s"%(hiveserver2,port,prinicipal)
        query = "beeline --silent=true --outputformat=csv2 -u \"%s\" -f %s"%(command,script_path)
        status,output = subprocess.getstatusoutput(query)
        if (status == 0):
            self.logger.info("executed script:%s successfully"%script_path)
        else:
            self.logger.debug("executed script,Loading to Hadoop:%s failed"%script_path)
            self.error.write("FLE001:executed script,Loading to Hadoop:%s failed"%script_path)
            self.error.close()
            time.sleep(1)
            post_validation.send_mail()
            sys.exit(1)
