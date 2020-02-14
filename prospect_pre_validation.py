#!/usr/bin/python
import re
import os
import sys
import shutil
import subprocess
import time

class ProspectPreValidation:
    """
    This Class will validate the all pre-validation checks for prospect application.
    """
    def __init__(self,logger,error_log,error_mail):
        """
        This constructor will enable the logger.
        """
        self.logger = logger
        self.error = error_log
        self.error_log = error_mail

    def monitor_ingestion_dir_for_info_file(self,prospect_ingestion_dir,post_validation):
        """
        starting point of the prospect application, which will monitor the prospect_ingestion directory for info file.
        """
        self.logger.info("in ProspectPreValidation class")
        self.logger.info("in ProspectPreValidation class,monitor_ingestion_dir_for_info_file method")
        self.logger.info("chekcing no of files present in ingestion dir:%s"%prospect_ingestion_dir)
        files = os.listdir(prospect_ingestion_dir)
        if (len(files) > 1):
            self.logger.info("total no of files present in ingestion dir:%s"%len(files))
            file_match = re.compile("(\S+)(\s+)-(\s+)Info.txt")
            filename = list(filter(file_match.match,files))
            if (len(filename) >= 1):
                try:
                    ret = re.search("(\S+)(\s+)-(\s+)Info.txt",filename[0])
                except IndexError:
                    self.error.write("FTE001:received info file in wrong format")
                    self.logger.debug("FTE001:received info file in wrong format")
                    self.error.close()
                    time.sleep(1)
                    post_validation.send_mail()
                    sys.exit(1)
                if (ret != None):
                    self.logger.info("info file received:%s"%filename[0])
                    self.error.write("received info file:%s"%filename[0])
                    time.sleep(1)
                    post_validation.send_mail()
                    return True
                else:
                    self.error.write("FTE001:received info file in wrong format")
                    self.logger.debug("FTE001:received info file in wrong format")
                    self.error.close()
                    time.sleep(1)
                    post_validation.send_mail()
                    sys.exit(1)
                    return False
            else:
                self.error.write("FTE001:no info files received")
                self.logger.debug("FTE001:no info files received")
                self.error.close()
                time.sleep(1)
                post_validation.send_mail()
                return False
    
    def monitor_ingestion_dir_for_success_file(self,prospect_ingestion_dir,aa_path,post_validation):
        """
        starting point of the prospect application, which will monitor the prospect_ingestion directory for success file.
        """
        self.logger.info("in ProspectPreValidation class")
        self.logger.info("in ProspectPreValidation class,monitor_ingestion_dir_for_success_file method")
        self.logger.info("chekcing no of files present in ingestion dir:%s"%prospect_ingestion_dir)
        files = os.listdir(prospect_ingestion_dir)
        print (len(files))
        if (len(files) >= 1):
            self.logger.info("total no of files present in ingestion dir:%s"%len(files))
            file_match = re.compile("(.*)_success.txt")
            filename = list(filter(file_match.match,files))
            if (len(filename) >= 1):
                self.logger.info("total no of success files present in ingestion dir:%s"%len(filename))
                for success_file in filename:
                    file = prospect_ingestion_dir+success_file
                    status,output = subprocess.getstatusoutput("mv %s %s"%(file,aa_path))
                    if (status == 0):
                        self.logger.info("success file:%s has been moved to:%s"%(file,aa_path))
                        
                        self.error.write("success file:%s has been recevied \n\n table is available in prospect_ingst.%s"%(file,filename[0]))
                        self.error.close()
                        post_validation.send_mail()
                        sys.exit(0)
                    else:
                        self.logger.debug("success file:%s has been moving has failed to:%s"%(file,aa_path))
                        self.error.write("FTE001:success file:%s has been moving has failed to:%s"%(file,aa_path))
                        self.logger.debug("FTE001:success file:%s has been moving has failed to:%s"%(file,aa_path))
                        self.error.close()
                        time.sleep(1)
                        post_validation.send_mail()
                        sys.exit(1)
            else:
                self.logger.info("total no of success files present in ingestion dir:%s"%len(filename))
                self.logger.info("no success files found in ingestion dir:%s"%prospect_ingestion_dir)
                       
    def monitor_ingestion_dir_for_failure_file(self,prospect_ingestion_dir,aa_path,database,post_validation,hiveserver2,port,prinicipal):
        """
        starting point of the prospect application, which will monitor the prospect_ingestion directory for failure file.
        """
        self.logger.info("in ProspectPreValidation class")
        self.logger.info("in ProspectPreValidation class,monitor_ingestion_dir_for_failure_file method")
        self.logger.info("chekcing no of files present in ingestion dir:%s"%prospect_ingestion_dir)
        files = os.listdir(prospect_ingestion_dir)
        if (len(files) >= 1):
            self.logger.info("total no of files present in ingestion dir:%s"%len(files))
            file_match = re.compile("(.*)_failure.txt")
            filename = list(filter(file_match.match,files))
            if (len(filename) >= 1):
                self.logger.info("total no of failure files present in ingestion dir:%s"%len(filename))
                for failure_file in filename:
                    file_fail = str(failure_file)
                    ret = re.search("(\S+)_sample_failure.txt",file_fail)
                    file_name = ret.group(1)
                    file = prospect_ingestion_dir+failure_file
                    status,output = subprocess.getstatusoutput("mv %s %s"%(file,aa_path))
                    if (status == 0):
                        self.logger.info("failure file:%s has been moved to:%s"%(file,aa_path))
                        self.backup_failure_prospect_table(file_name,database,post_validation,hiveserver2,port,prinicipal)
                    else:
                        self.logger.debug("failure file:%s has been moving has failed to:%s"%(file,aa_path))
                        self.error.write("FTE001:failure file:%s has been moving has failed to:%s"%(file,aa_path))
                        self.logger.debug("FTE001:failure file:%s has been moving has failed to:%s"%(file,aa_path))
                        self.error.close()
                        time.sleep(1)
                        post_validation.send_mail()
                        sys.exit(1)
            else:
                self.logger.info("total no of failure files present in ingestion dir:%s"%len(filename))
                self.logger.info("no failure files found in ingestion dir:%s"%prospect_ingestion_dir)

    def backup_failure_prospect_table(self,file_name,database,post_validation,hiveserver2,port,prinicipal):
        """
        This method will take the backup_failure_prospect_table.
        """
        table = file_name.lower()
        string = "failure"
        bkp_tbl = table+"_"+string
        self.logger.info("received failure file:%s, we need to take backup"%file_name)
        self.logger.info("in backup_failure_prospect_table")
        command = "jdbc:hive2://%s:%s/;principal=%s"%(hiveserver2,port,prinicipal)
        query = "set tez.queue.name=dca;alter table %s.%s rename to %s.%s"%(database,table,database,bkp_tbl)
        query = "beeline --silent=true --outputformat=csv2 -u \"%s\" -e \"%s\""%(command,query)
        self.logger.info("query executing %s"%query)
        status,output = subprocess.getstatusoutput(query)
        if (status == 0):
            self.logger.info("query executed successfully:%s"%query)
            self.logger.info("created backup table %s:%s"%(database,bkp_tbl))
            self.error.write("created backup table %s:%s"%(database,bkp_tbl))
            self.error.close()
            time.sleep(1)
            self.send_mail()
            sys.exit(1)
        else:
            self.logger.debug("query execution failed,creating backup table for failed prospect file:%s"%query)
            self.error.write("FTE100:query execution failed,creating backup table for failed file:%s"%query)
            self.error.close()
            time.sleep(1)
            self.send_mail()
            sys.exit(1)
                   
    def get_prospect_file_name(self,prospect_ingestion_dir,post_validation):
        """
        this method will return the file name received.
        """
        self.logger.info("in ProspectPreValidation class,get_prospect_file_name method")
        files = os.listdir(prospect_ingestion_dir)
        if (len(files) > 1):
            file_match = re.compile("(\S+)(\s+)-(\s+)Info.txt")
            filename = list(filter(file_match.match,files))
            ret = re.search("(\S+)(\s+)-(\s+)Info.txt",filename[0])
            file_name = ret.group(1)
            if (ret != None):
                self.logger.info("filename received:%s"%file_name)
                #file = file_name.replace("-","_")
                return file_name
            else:
                self.logger.debug("filename not received:%s"%file_name)
                self.error.write("FTE001:received info file in wrong format")
                self.logger.debug("FTE001:received info file in wrong format")
                self.error.close()
                time.sleep(1)
                post_validation.send_mail()
                sys.exit(1)
                return False
    
    def rename_prospect_info_file(self,prospect_ingestion_dir,post_validation):
        """
        this method will rename the info file as Info file has spaces in its file name.
        """
        self.logger.info("in ProspectPreValidation class,rename_prospect_info_file")
        files = os.listdir(prospect_ingestion_dir)
        if (len(files) > 1):
            file_match = re.compile("(\S+)(\s+)-(\s+)Info.txt")
            filename = list(filter(file_match.match,files))
            try:
                ret = re.search("(\S+)(\s+)-(\s+)Info.txt",filename[0])
            except IndexError:
                self.logger.debug("filename not received:%s"%file_name)
                self.error.write("FTE001:received info file in wrong format,Rename Failed")
                self.logger.debug("FTE001:received info file in wrong format,Rename Failed")
                self.error.close()
                time.sleep(1)
                post_validation.send_mail()
                sys.exit(1)

            match_string = ret.group(1)
            os.rename(os.path.join(prospect_ingestion_dir, filename[0]), os.path.join(prospect_ingestion_dir, filename[0].replace(' ', '-')))
            renamed_file =  os.path.join(prospect_ingestion_dir, filename[0].replace(' ', '-'))
            self.logger.info("renamed info file:%s"%renamed_file)
            return renamed_file
        else:
            self.logger.debug("filename not received:%s"%file_name)
            self.error.write("FTE001:received info file in wrong format,Rename Failed")
            self.logger.debug("FTE001:received info file in wrong format,Rename Failed")
            self.error.close()
            time.sleep(1)
            post_validation.send_mail()
            sys.exit(1)
            return False 

    def pre_validation(self,info_file,file_name,prospect_ingestion_dir,prospect_staging_dir,post_validation):
        """
        this method will read the renamed info file and count the no of part files if directory has.
        """
        self.logger.info("in ProspectPreValidation class,pre_validation method")
        self.logger.info("open info file:%s"%info_file)
        list_files = []
        file_read = open(info_file,"r")
        search_string = file_name+"."
        for line in file_read:
            if search_string in line:
                list_files.append(line)
        if (len(list_files) > 4):
             self.logger.info("As len(list_files) more than 4, so part files received")
             self.logger.info("calling read_md5_record_count method")
             zip_part_md5,zip_md5,csv_md5 = self.read_md5_record_count(list_files,prospect_ingestion_dir,info_file,post_validation)
             self.logger.info("calling validate_count_partfiles_ingestion_dir")
             status = self.validate_count_partfiles_ingestion_dir(zip_part_md5,prospect_ingestion_dir,file_name,post_validation)
             if (status == True):
                 self.logger.info("part file count matched between info and ingestion directory")
                 self.logger.info("calling create_dir_move_files_to_staging_dir method")
                 status,staging_dir = self.create_dir_move_files_to_staging_dir(prospect_ingestion_dir,file_name,prospect_staging_dir,post_validation)
                 self.logger.info("all files moved to staging dir, now we need to validate md5")
                 if (status == True):
                     self.logger.info("calling validate_md5_part_files method")
                     self.validate_md5_part_files(file_name,zip_part_md5,staging_dir,zip_md5,csv_md5,post_validation)
                     self.logger.info("calling zip_part_files_md5_validation method")
                     self.zip_part_files_md5_validation(file_name,staging_dir,zip_md5,post_validation)
                     self.logger.info("calling unzip_md5_validation_csv method")
                     self.unzip_md5_validation_csv(file_name,staging_dir,csv_md5,post_validation)
              
        elif (len(list_files) <= 4):
             self.logger.info("As len(list_files) less than or equal to 4, so no part files received")
             self.logger.info("calling read_md5_record_count method")
             zip_md5,csv_md5 = self.read_md5_record_count(list_files,prospect_ingestion_dir,info_file,post_validation)
             self.logger.info("calling create_dir_move_files_to_staging_dir method")
             status,staging_dir = self.create_dir_move_files_to_staging_dir(prospect_ingestion_dir,file_name,prospect_staging_dir,post_validation)
            
             if (status == True):
                 self.logger.info("calling zip_part_files_md5_validation method")
                 self.zip_md5_validation(file_name,staging_dir,zip_md5,post_validation)
                 self.logger.info("calling unzip_md5_validation_csv method")
                 self.unzip_md5_validation_csv(file_name,staging_dir,csv_md5,post_validation)
                 self.logger.info("prospect pre validation completed")
            

    def read_md5_record_count(self,list_files,prospect_ingestion_dir,info_file,post_validation):
        """
        this method will return md5 for part_file,zip,csv files in info file.
        """
        self.logger.info("in ProspectPreValidation,read_md5_record_count method")
        csv_md5 = {}
        zip_md5 = {}
        zip_part_md5 = {}
        if (len(list_files) > 4):
            for file_md5 in list_files:
                try:
                    ret = re.search("(\s+)(\S+)(\s+)(\S+)",file_md5)
                except IndexError:
                    self.logger.debug("filename not received:%s"%file_name)
                    self.error.write("FTE001:received info file in wrong format,Md5 string not matched")
                    self.logger.debug("FTE001:received info file in wrong format,Md5 string not matched")
                    self.error.close()
                    time.sleep(1)
                    post_validation.send_mail()
                    sys.exit(1)
                if (len(ret.group(4)) > 31) and (re.search(".csv$",ret.group(2))):
                    csv_name = str(ret.group(2)).replace(":","")
                    csv_md5[csv_name] = ret.group(4)
                elif (len(ret.group(4)) > 31) and (re.search(".zip$",ret.group(2))):
                    zip_name = str(ret.group(2)).replace(":","")
                    zip_md5[zip_name] = ret.group(4)
                elif (len(ret.group(4)) > 31) and (re.search(".zip.",ret.group(2))):
                    zip_part_md5[ret.group(2)] = ret.group(4)
            self.logger.info("retuning md5 values for partfile,zip,csv file from info file:%s"%info_file)
            return zip_part_md5,zip_md5,csv_md5
    
        elif (len(list_files) <= 4):
            for file_md5 in list_files:
                try:
                    ret = re.search("(\s+)(\S+)(\s+)(\S+)",file_md5)
                except IndexError:
                    self.logger.debug("received info file in wrong format,Md5 string not matched:%s"%file_name)
                    self.error.write("FTE001:received info file in wrong format,Md5 string not matched")
                    self.logger.debug("FTE001:received info file in wrong format,Md5 string not matched")
                    self.error.close()
                    time.sleep(1)
                    post_validation.send_mail()
                    sys.exit(1)
                if (len(ret.group(4)) > 31) and (re.search(".csv$",ret.group(2))):
                    csv_name = str(ret.group(2)).replace(":","")
                    csv_md5[csv_name] = ret.group(4)
                elif (len(ret.group(4)) > 31) and (re.search(".zip$",ret.group(2))):
                    zip_name = str(ret.group(2)).replace(":","")
                    zip_md5[zip_name] = ret.group(4)
            self.logger.info("retuning md5 values for zip,csv file from info file:%s"%info_file)
            return zip_md5,csv_md5
            
    def validate_count_partfiles_ingestion_dir(self,zip_part_md5,prospect_ingestion_dir,file_name,post_validation):
        """
        this method will validate the no of part files in info file and ingestion_dir.
        """
        self.logger.info("in ProspectPreValidation, validate_count_partfiles_ingestion_dir")
        count = 0
        search = file_name+"."+"zip"+"."
        files = os.listdir(prospect_ingestion_dir)
        for file in files:
            if search in file:
                count = count+1
        count_str = str(count)
        self.logger.info("no of part files found in the ingestion dir %s:%s"%(prospect_ingestion_dir,count_str))
        self.logger.info("no of part files found in the info file %s:%s"%(prospect_ingestion_dir,str(len(zip_part_md5))))
        if ((count!=0) and (count==len(zip_part_md5))):
            self.logger.info("part file count matched ingestion_dir vs Info file")
            return True
        else:
            self.logger.debug("part file count not matched ingestion_dir vs Info file")
            self.error.write("FTE001:part file count not matched ingestion_dir vs Info file")
            self.logger.debug("FTE001:part file count not matched ingestion_dir vs Info file")
            self.error.close()
            time.sleep(1)
            post_validation.send_mail()
            sys.exit(1)
            return False
    
    def create_dir_move_files_to_staging_dir(self,prospect_ingestion_dir,file_name,prospect_staging_dir,post_validation):
        """
        this method will create directory in staging directory and will move all files to staging dir.
        """
        self.logger.info("in ProspectPreValidation,create_dir_move_files_to_staging_dir method")
        file_name_lower = file_name.lower()
        staging_dir = prospect_staging_dir+file_name_lower
        file_dir = prospect_ingestion_dir+file_name+"*"
        try:
            os.makedirs(staging_dir)
        except OSError:  
            self.logger.debug("creation of dir failed:%s"%staging_dir)
            self.error.write("FTE001:creation of dir failed:%s"%staging_dir)
            self.logger.debug("FTE001:creation of dir failed:%s"%staging_dir)
            self.error.close()
            time.sleep(1)
            post_validation.send_mail()
            sys.exit(1)
        else:  
            self.logger.info("creation of dir successful:%s"%staging_dir)
            self.logger.info("moving all specific files to staging dir:%s"%staging_dir)
            status,output = subprocess.getstatusoutput("mv %s %s"%(file_dir,staging_dir))
            if (status == 0):
                self.logger.info("all files moved successfully to staging dir:%s"%staging_dir)
                return True,staging_dir
            else:
                self.logger.debug("files moving to staging dir failed:%s"%staging_dir)
                self.error.write("FTE001:files moving to staging dir failed:%s"%staging_dir)
                self.logger.debug("FTE001:files moving to staging dir failed:%s"%staging_dir)
                self.error.close()
                time.sleep(1)
                post_validation.send_mail()
                sys.exit(1)
                return False,False
    
    def validate_md5_part_files(self,file_name,zip_part_md5,staging_dir,zip_md5,csv_md5,post_validation):
        """
        this method will validate the md5 for part files.
        """
        self.logger.info("in ProspectPreValidation,validate_md5_part_files method")
        count = 0
        search = file_name+"."+"zip"+"."
        files = os.listdir(staging_dir)
        for file in files:
            if search in file:
                count = count+1
        if (count==len(zip_part_md5)):
            self.logger.info("part files count matched between staging dir and info file")
            for key in zip_part_md5:
                file_name = key.replace(":","")
                part_file = os.path.join(staging_dir,file_name)
                status,output = subprocess.getstatusoutput("md5sum %s"%part_file)
                if (status == 0):
                    ret = re.search("(\S+)(\s+)(\S+)",output)
                    if (zip_part_md5[key]== str(ret.group(1)).upper()):
                        self.logger.info("md5 value matched file %s:,md5_sum : %s, md5_info_file:%s"%(part_file,str(ret.group(1)).upper(),zip_part_md5[key]))
                    else:
                        self.logger.debug("md5 value not matched file %s:,md5_sum : %s, md5_info_file:%s"%(part_file,str(ret.group(1)).upper(),zip_part_md5[key]))
                        self.error.write(" FTE001:md5 value not matched file %s:,md5_sum : %s, md5_info_file:%s"%(part_file,str(ret.group(1)).upper(),zip_part_md5[key]))
                        self.logger.debug("FTE001:md5 value not matched file %s:,md5_sum : %s, md5_info_file:%s"%(part_file,str(ret.group(1)).upper(),zip_part_md5[key]))
                        self.error.close()
                        time.sleep(1)
                        post_validation.send_mail()
                        sys.exit(1)
        else:
            self.logger.debug("part file count not matched ingestion_dir vs Info file")
            self.error.write("FTE001:part file count not matched ingestion_dir vs Info file")
            self.logger.debug("FTE001:part file count not matched ingestion_dir vs Info file")
            self.error.close()
            time.sleep(1)
            post_validation.send_mail()
            sys.exit(1)

    def zip_part_files_md5_validation(self,file_name,staging_dir,md5_zip,post_validation):
        """
        This methid will zip all part files and validate md5 for zip file.
        """
        self.logger.info("in ProspectPreValidation,zip_part_files_md5_validation method")
        search_string_part = file_name+"."+"zip."+"0*"
        search_string = file_name+"."+"zip"
        part_files = os.path.join(staging_dir,search_string_part)
        part_file = os.path.join(staging_dir,search_string)
        self.logger.info("partfiles:%s"%part_files)
        self.logger.info("zipfile:%s"%part_file)
        status,output = subprocess.getstatusoutput("cat %s>%s"%(part_files,part_file))
        if (status == 0):
            self.logger.info("part files merged successfully created zipfile:%s"%part_file)
            self.logger.info("calculating md5 checksum for created zipfile:%s"%part_file)
            status,output = subprocess.getstatusoutput("md5sum %s"%part_file)
            if (status == 0):
                ret = re.search("(\S+)(\s+)(\S+)",output)
                for key in md5_zip:
                    if (md5_zip[key]== str(ret.group(1)).upper()):
                        self.logger.info("md5 checksum for created zipfile:%s"%str(ret.group(1)).upper())
                        self.logger.info("md5 checksum in infile for zipfile:%s"%md5_zip[key])
                        self.logger.info("md5 checksum matched for zip file and info file checksum value")
                    else:
                        self.logger.debug("md5 checksum for created zipfile:%s"%str(ret.group(1)).upper())
                        self.logger.debug("md5 checksum for zipfile in Info File:%s"%md5_zip[key])
                        self.error.write("FTE001:md5 checksum not matched for zip file:%s"%part_file)
                        self.logger.debug("FTE001:md5 checksum for created zipfile:%s"%str(ret.group(1)).upper())
                        self.logger.debug("FTE001:md5 checksum for zipfile in Info File:%s"%md5_zip[key])
                        self.error.close()
                        time.sleep(1)
                        post_validation.send_mail()
                        sys.exit(1)
        else:
            self.logger.debug("ZIP file creation failed from part files:%s"%file_name)
            self.error.write("FTE001:ZIP file creation failed from part files:%s"%file_name)
            self.logger.debug("FTE001:ZIP file creation failed from part files:%s"%file_name)
            self.error.close()
            time.sleep(1)
            post_validation.send_mail()
            sys.exit(1)

    def unzip_md5_validation_csv(self,file_name,staging_dir,md5_csv,post_validation):
        """
        This method will unzip to csv file and validate md5 for csv file
        """
        self.logger.info("in ProspectPreValidation,unzip_md5_validation_csv method")
        search_string = file_name+"."+"zip"
        zip_file = os.path.join(staging_dir,search_string)
        self.logger.info("zip file to unzip:%s"%zip_file)
        csv_fd = search_string.replace("zip","csv")
        csv_file = os.path.join(staging_dir,csv_fd)
        self.logger.info("generating csv file:%s"%csv_file)
        status,output = subprocess.getstatusoutput("unzip %s -d %s"%(zip_file,staging_dir))
        if (status == 0):
            self.logger.info("generated csv file successfully:%s"%csv_file)
            self.logger.info("calculating md5 for csv_file:%s"%csv_file)
            status,output = subprocess.getstatusoutput("md5sum %s"%csv_file)
            if (status == 0):
                ret = re.search("(\S+)(\s+)(\S+)",output)
                for key in md5_csv:
                    if (md5_csv[key]== str(ret.group(1)).upper()):
                        self.logger.info("md5 for csv_file:%s"%str(ret.group(1)).upper())
                        self.logger.info("md5 for csv file in info file:%s"%md5_csv[key])
                        self.logger.info("md5 checksum matched for csv_file:%s"%csv_file)
                    else:
                        self.logger.debug("md5 checksum for csv file %s:%s"%(str(ret.group(1)).upper(),csv_file))
                        self.logger.debug("md5 checksum for csv file in for file %s:%s"%(md5_csv[key],csv_file))
                        self.error.write("FTE001:md5 checksum Not matched for csv file:%s"%csv_file)
                        self.logger.debug("FTE001:md5 checksum for csv file %s:%s"%(str(ret.group(1)).upper(),csv_file))
                        self.logger.debug("FTE001:md5 checksum for csv file in for file %s:%s"%(md5_csv[key],csv_file))
                        self.logger.debug("FTE001:md5 checksum Not matched for csv file:%s"%csv_file)
                        self.error.close()
                        time.sleep(1)
                        post_validation.send_mail()
                        sys.exit(1)

    def zip_md5_validation(self,file_name,staging_dir,md5_zip,post_validation):
        """
        This method will calculate the md5 sum for zip file
        """
        self.logger.info("in ProspectPreValidation,zip_md5_validation method")
        search_string = file_name+"."+"zip"
        zip_file = os.path.join(staging_dir,search_string)
        self.logger.info("zip file:%s"%zip_file)
        self.logger.info("calculating md5 for zip_file:%s"%zip_file)
        status = os.path.isfile(zip_file)
        if (status == True):
            self.logger.info("ZIP file found:%s"%zip_file)
            status,output = subprocess.getstatusoutput("md5sum %s"%zip_file)
            if (status == 0):
                ret = re.search("(\S+)(\s+)(\S+)",output)
                for key in md5_zip:
                    if (md5_zip[key]== str(ret.group(1)).upper()):
                        self.logger.info("md5 for zip_file:%s"%str(ret.group(1)).upper())
                        self.logger.info("md5 for zip file in info file:%s"%md5_zip[key])
                        self.logger.info("md5 checksum matched for zip_file:%s"%zip_file)
                    else:
                        self.logger.debug("md5 for zip_file:%s=%s"%(str(ret.group(1)).upper(),zip_file))
                        self.logger.debug("md5 for zip file in info file:%s=%s"%(md5_zip[key],zip_file))
                        self.logger.debug("FTE001:md5 for zip file not matched:%s"%(md5_zip[key]))
                        self.error.write("FTE001:md5 for zip file not matched:%s"%(md5_zip[key]))
                        self.logger.debug("FTE001:md5 for zip_file:%s=%s"%(str(ret.group(1)).upper(),zip_file))
                        self.logger.debug("FTE001:md5 for zip file in info file:%s=%s"%(md5_zip[key],zip_file))
                        self.logger.debug("FTE001:md5 for zip file not matched:%s"%(md5_zip[key]))
                        self.error.close()
                        time.sleep(1)
                        post_validation.send_mail()
                        sys.exit(1)
        else:
            self.logger.debug("ZIP file not found:%s"%zip_file)
            self.logger.debug("FTE001:ZIP file not found:%s"%zip_file)
            self.error.write("FTE001:ZIP file not found:%s"%zip_file)
            self.error.close()
            time.sleep(1)
            post_validation.send_mail()
            sys.exit(1)
