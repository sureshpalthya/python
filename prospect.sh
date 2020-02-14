#!/bin/sh
kdestroy
kinit -VV -kt /home/diuser/keytabs/diuser.keytab diuser@DCAPROD.CORP.SPRINT.COM
klist

mail_list="rammohanreddy.muthaparapu@sprint.com"

cd /data/apps/0bq/ingestion/prospect/bin/
/data/apps/python-interpreter/anaconda3.5/bin/python3.6 prospect_main.py

STATUS=$?
echo $STATUS
if [[ $STATUS -eq 0 ]]
then
    echo 0 "Prospect Automation executed successfully"
    echo "Prospect Automation succedded" | mailx -s "Prospect Automation succedded" ${mail_list}
    exit 0
else
    echo "Prospect Automation failed" | mailx -s "Prospect Automation failed" ${mail_list}
    exit 1
fi
