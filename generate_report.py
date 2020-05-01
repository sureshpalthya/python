#!/usr/bin/env python
# coding: utf-8

# In[1]:
import pandas as pd
import numpy as np
import plotly.plotly as py
import plotly.offline as offline
from datetime import datetime, timedelta
offline.init_notebook_mode(connected=True)
import plotly.figure_factory as ff
from pyspark.sql import SparkSession, column
from pyspark.sql.types import StringType
from datetime import datetime, timedelta
import bs4
import os

# In[2]:
spark = SparkSession.builder.appName("Grantt Report").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
print("Session Successful")
print("Query Starts")

dir='/data/apps/0zy/scripts/operations/outputs/'

import argparse

try:
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", help="Enter Email")
    parser.add_argument("--date", help="Enter Date")
    args = parser.parse_args()
    msg_txt=""
    
    if args.id:
        p_email = args.id.strip()
    if args.date:
        run_dt = "'"+args.date.strip()+"'"
        run_dt_rpt= run_dt
    else:
        run_dt = 'date_sub(current_date, 14)'
        s = datetime.today() - timedelta(days=14)
        run_dt_rpt = s.strftime("%Y-%m-%d")
    
    print("*"*25)
    print("RUN DT: "+run_dt_rpt)
    print("Email ID: "+p_email)
    print("*"*25)
    
except Exception as e:
    print(" In exception Parameter Section ",str(e))

try:
    print("SELECT * FROM pme_0zy_cntl.execution_detail WHERE 1=1 AND substr(executiontimestamp,1,10) >="+run_dt)
    sqlDF = spark.sql("SELECT * FROM pme_0zy_cntl.execution_detail WHERE 1=1 AND substr(executiontimestamp,1,10) >="+run_dt)
    sqlDF.show()
    care_df=sqlDF.select("*").toPandas()
    care_df=care_df.loc[care_df['status']!='InProgress']
    if(len(care_df)>0):
        care_df.executiontimestamp.replace('?',np.nan)
        care_df['executiontimestamp']=pd.to_datetime(care_df.executiontimestamp)
        care_df.end_time.replace('?',np.nan)
        care_df['end_time']=pd.to_datetime(care_df.end_time)
        n=care_df.apply(lambda x:(x['end_time']-x['executiontimestamp']),axis=1)
        care_df['duration']=n.dt.seconds/60
        care_df['execution_id']=care_df.execution_id.apply(lambda x:x[0:x.find('|')])
    else:
        print('No Data')
except Exception as e:
    print(" In exception Care DF Section ",str(e))


# In[ ]:

try:
    print("SELECT * FROM pme_0zy_dwh.execution_retail_detail WHERE 1=1 AND substr(executiontimestamp,1,10) >="+run_dt)
    sqlDF = spark.sql("SELECT * FROM pme_0zy_dwh.execution_retail_detail WHERE 1=1 AND substr(executiontimestamp,1,10) >="+run_dt)
    sqlDF.show()
    retail_df=sqlDF.select("*").toPandas()
    retail_df=retail_df.loc[retail_df['status']!='InProgress']
    if(len(retail_df)>0):
        retail_df.executiontimestamp.replace('?',np.nan)
        retail_df['executiontimestamp']=pd.to_datetime(retail_df.executiontimestamp)
        retail_df.end_time.replace('?',np.nan)
        retail_df['end_time']=pd.to_datetime(retail_df.end_time)
        n=retail_df.apply(lambda x:(x['end_time']-x['executiontimestamp']),axis=1)
        retail_df['duration']=n.dt.seconds/60
        retail_df['execution_id']=retail_df.execution_id.apply(lambda x:x[0:x.find('|')])
    else:
        print('No Data')
except Exception as e:
    print(" In exception Retail DF Section ",str(e))

# In[5]:


## Y - Axis Values
def y_axis(care_df):
    care_df['Task']=care_df.executiontimestamp.dt.date
    care_df['Task']=pd.to_datetime(care_df['Task'])

def x_axis(care_df):
    care_df['fdt_check']=care_df.apply(lambda x: 0 if(x.executiontimestamp.day==x.end_time.day) else 1,axis=1)
    care_df['Start']=care_df.executiontimestamp.dt.time
    care_df['Start']=pd.to_datetime(care_df['Start'],format= '%H:%M:%S')
    care_df['Finish']=care_df.end_time.dt.time
    care_df['Finish']=pd.to_datetime(care_df['Finish'],format= '%H:%M:%S')
    care_df['Finish']=care_df.apply(lambda x: x['Finish']+timedelta(1) if(x.fdt_check>0) else x['Finish'],axis=1)

## Annotations
def annotations(care_df):
    care_df.sort_values(['Task'],ascending=False,inplace=True)
    ann_list=[]
    for outer_index,x in enumerate(care_df.Task.dt.date.unique().tolist()):
       for innner_index,row in care_df.loc[care_df['Task']==x].iterrows():
    #         print(row.Start,outer_index)
            ann_list+=[dict(x=row.Start,y=outer_index,arrowhead=2,arrowsize=1,arrowwidth=0.3,text= row.execution_id+' - '+str(round(row.duration))+' Min',  font=dict(family = 'Arial',size = 9,color = 'grey'))]
    return ann_list

def gt_model(care_df,msg):
    fig = ff.create_gantt(care_df,
                          title=msg+str(care_df.executiontimestamp.dt.date.min()),
                      colors=dict(COMPLETED='rgb(143, 188, 143)',FAILED="rgb(220,20,60)"), 
                      index_col='status',
                      show_colorbar=True,
                       bar_width=0.1,
                      group_tasks=True,
                      showgrid_x=True, 
                      showgrid_y=True)
    return fig


html_string = '''
        <html>
          <link rel="stylesheet" type="text/css" href="style.css"/>
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
          <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.4.4/jquery.js"></script>
    <script type="text/javascript">
    {x}
    </script>
          <style >
          span {sp}
        {m}
        </style>
        <body>
        <br>{graph}{g_script}
          </body>
        </html>
        '''

html_string1 = '''
        <html>
          <link rel="stylesheet" type="text/css" href="style.css"/>
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
          <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.4.4/jquery.js"></script>
    <script type="text/javascript">
    {x}
    </script>
          <style >
          span {sp}
        {m}
        </style>
        <body>
        <br>{graph1}{g_script1}
          </body>
        </html>
        '''      

try:
    if(len(care_df)>0):
        y_axis(care_df)
        x_axis(care_df)
        ann_li=annotations(care_df)
        fig=gt_model(care_df,msg='Care Jobs(Batch 1/2) from ')
        fig['layout']['annotations'] = ann_li
        fig["layout"]["xaxis"] = {'tickfont':{'family':'Courier','size':15},
               'tickformat' : '%H-%M',"gridcolor": "rgb(159, 197, 232)"}
        fig['layout'].update(autosize=False, width=990, height=950)
        offline.plot(fig,show_link=False,output_type='file',filename=dir+'care.html',include_plotlyjs = False,auto_open = False,image_filename='plot_image', image_width=900, image_height=900)
        
        with open(dir+'care.html') as inf:
            txt = inf.read()
            soup = bs4.BeautifulSoup(txt)
            
        div_tag=str(soup.find("div"))
        div_tag=div_tag.replace("100%"," ")
        script_tag=str(soup.find("script"))
        
        with open(dir+'iperform_care_jobs.html', 'w') as f:
            xyz=f.write(html_string.format(x="$(document).ready(function(){$('th').each(function(){if ($(this).text() == 'InProgress') {$(this).css('background-color','#FFFF99'); } if ($(this).text() == 'diff') {$(this).css('background-color','crimson');}});});",
                                           p="{font-size: 9pt; font-family: Courier New;}",graph=div_tag,g_script=script_tag, sp="{font-size: 9pt; font-family: Courier New;background-color:yellow;}",
                                           m=".legend_color{display:inline-block;height:13px;width:13px;margin-left:8px} .modebar-container{display:none} .dataframe {font-size: 11pt; font-family: Courier New;border-collapse: collapse; border: 1px solid black;} .dataframe thead{background: #1C5AAB;font-size: 12pt;text-align: center;color:white} tbody{text-align: left;background: #ECEEF4}.plotly-graph-div{height: 950px; width: 120%;;margin:auto}"
                                        ))
    
except Exception as e:
    print(" In exception Care Report Section ",str(e))


# In[12]:
try:
    if(len(retail_df)>0):
        y_axis(retail_df)
        x_axis(retail_df)
        ann_li1=annotations(retail_df)
        fig1=gt_model(retail_df,msg='Retail jobs from ')
        fig1['layout']['annotations'] = ann_li1
        fig1["layout"]["xaxis"] = {'tickfont':{'family':'Courier','size':15},
               'tickformat' : '%H-%M',"gridcolor": "rgb(159, 197, 232)"}
        fig1['layout'].update(autosize=False, width=990, height=950)
        offline.plot(fig1,show_link=False,output_type='file',filename=dir+'retail.html',include_plotlyjs = False,auto_open = False,image_filename='plot_image', image_width=900, image_height=900)
        
        with open(dir+'retail.html') as inf1:
            txt1 = inf1.read()
            soup1 = bs4.BeautifulSoup(txt1)
    
        div_tag1=str(soup1.find("div"))
        div_tag1=div_tag1.replace("100%"," ")
        script_tag1=str(soup1.find("script"))
        
        with open(dir+'iperform_retail_jobs.html', 'w') as f:
            xyz=f.write(html_string1.format(x="$(document).ready(function(){$('th').each(function(){if ($(this).text() == 'InProgress') {$(this).css('background-color','#FFFF99'); } if ($(this).text() == 'diff') {$(this).css('background-color','crimson');}});});",
                                           p="{font-size: 9pt; font-family: Courier New;}",graph1=div_tag1,g_script1=script_tag1, sp="{font-size: 9pt; font-family: Courier New;background-color:yellow;}",
                                           m=".legend_color{display:inline-block;height:13px;width:13px;margin-left:8px} .modebar-container{display:none} .dataframe {font-size: 11pt; font-family: Courier New;border-collapse: collapse; border: 1px solid black;} .dataframe thead{background: #1C5AAB;font-size: 12pt;text-align: center;color:white} tbody{text-align: left;background: #ECEEF4}.plotly-graph-div{height: 950px; width: 120%;;margin:auto}"
                                        ))
    
except Exception as e:
    print(" In exception Retail Report Section ",str(e))
    
try:
    if (os.path.isfile(dir+'iperform_retail_jobs.html') and os.path.isfile(dir+'iperform_care_jobs.html')):
    #     print(os.system('echo PFA, report for CARE jobs from | mail -s iPerform Care Jobs -a iperform_care_jobs.html -r iPerformCare@sprint.com '+p_email))
        print(os.system('echo "PFA, report for Retail jobs from (Run Date:"'+run_dt_rpt+'")" | mail -s "iPerform Retail Jobs" -a '+dir+'iperform_retail_jobs.html -r iPerformRetail@sprint.com '+p_email))
        print(os.system('echo "PFA, report for Care jobs from (Run Date:"'+run_dt_rpt+'")" | mail -s "iPerform Care Jobs" -a '+dir+'iperform_care_jobs.html -r iPerformCare@sprint.com '+p_email))
    elif (os.path.isfile(dir+'iperform_retail_jobs.html')):
        print(os.system('echo "PFA, report for Retail jobs from (Run Date:"'+run_dt_rpt+'")" | mail -s "iPerform Retail Jobs" -a '+dir+'iperform_retail_jobs.html -r iPerformRetail@sprint.com '+p_email))
    elif (os.path.isfile(dir+'iperform_care_jobs.html')):
        print(os.system('echo "PFA, report for Care jobs from (Run Date:"'+run_dt_rpt+'")" | mail -s "iPerform Care Jobs" -a '+dir+'iperform_care_jobs.html -r iPerformCare@sprint.com '+p_email))
except Exception as e:
    print(" In exception Sending Report ",str(e))
    


    # with open('/production/p6v/operations/outputs/nrt.html', 'w') as f:
