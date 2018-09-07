import pickle
import time
import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import datetime as dt
from zdesk import Zendesk, RateLimitError, ZendeskError
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.zendesk_hook import ZendeskHook
from pprint import pprint
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
import os


today_date = dt.datetime.now()
today_date_str=today_date.strftime("%m-%d")

time_back_val=5
time_back_unit='days'

"""
The convert_time_delta_to_unix function below converts the variables above to unix time,
which is required by the Zendesk Path/Zendesk Hook. The next lines uses the function to create the unix time and make
the call with the Zendesk_hook
"""
def convert_time_delta_to_unix (unit,time_back):
    if unit=='days':
        delta=dt.timedelta(days=time_back)
    else:
        delta=dt.timedelta(hours=time_back) #expect user to input hours, unless they specify days
    days_back_datetime=dt.datetime.now()-delta
    days_back_datetime_str=days_back_datetime.strftime("%Y/%m/%d %H:%M")
    unix_time_str= time.mktime(dt.datetime.strptime(days_back_datetime_str, "%Y/%m/%d %H:%M").timetuple())
    return unix_time_str


def zendesk_hook_call (connection_id, path_str):
    zendesk_hook=ZendeskHook('connection_id')
    json_response=zendesk_hook.call(path)
    ticket_lst=json_response['tickets']
    return ticket_lst

def transform (json_lst):
    client_type_id=46866927
    client_custom_id=47830767
    kiosk_custom_id=360000762913
    root_cause_id=56708627
    data_main=[]
    data_client=[]
    data_kiosk=[]
    data_rootcause=[]
    data_client_type=[]

    for ticket in json_lst:
        data_main.append(ticket)
        custom_fields = next((fields for fields in ticket['custom_fields'] if fields['id'] == client_custom_id), None)
        custom_fields['url']=ticket['url']
        custom_fields['client_name'] = custom_fields.pop('value')
        data_client.append(custom_fields)
        #print (match['value'])
        custom_fields2 = next((fields for fields in ticket['custom_fields'] if fields['id'] == kiosk_custom_id), None)
        custom_fields2['url']=ticket['url']
        custom_fields2['kiosk_name'] = custom_fields2.pop('value')
        data_kiosk.append(custom_fields2)
        custom_fields3 = next((fields for fields in ticket['custom_fields'] if fields['id'] == root_cause_id), None)
        custom_fields3['url']=ticket['url']
        custom_fields3['root_cause'] = custom_fields3.pop('value')
        data_rootcause.append(custom_fields3)
        custom_fields4 = next((fields for fields in ticket['custom_fields'] if fields['id'] == client_type_id), None)
        custom_fields4['url']=ticket['url']
        custom_fields4['client_type'] = custom_fields4.pop('value')
        data_client_type.append(custom_fields4)

    data_client_norm=json_normalize(data_client)
    data_client_norm = data_client_norm.drop('id', 1)
    data_kiosk_norm=json_normalize(data_kiosk)
    data_kiosk_norm = data_kiosk_norm.drop('id', 1)
    data_rootcause_norm=json_normalize(data_rootcause)
    data_rootcause_norm = data_rootcause_norm.drop('id', 1)
    data_client_type_norm=json_normalize(data_client_type)
    data_client_type_norm = data_client_type_norm.drop('id', 1)

    data_norm=json_normalize(data_main, record_path=['priority'],\
                             meta=['priority','status','subject','url','tags','description','updated_at',\
                                              ['via','source','from','name'],'created_at','client_type'], \
                             errors='ignore')

    df_merge2=pd.merge(data_norm, data_client_norm, on='url', how='left')
    df_merge3=pd.merge(df_merge2, data_rootcause_norm, on='url', how='left')
    df_merge4=pd.merge(df_merge3, data_client_type_norm, on='url', how='left')
    df_final=pd.merge(df_merge4, data_kiosk_norm, on='url', how='left')

    df_final['kiosk_normalized']=df_final['kiosk_name'].str.extract("(?:.*__)(.*)$")
    df_final['kiosk_normalized_error']=df_final['kiosk_name'].str.extract("(?:.*_)(.*)$")
    df_final['kiosk_normalized'].fillna(df_final['kiosk_normalized_error'], inplace=True)
#     df_final2=pd.merge(df_final, df_kiosk_match, on='kiosk_normalized', how='left')
    df_final2=df_final
    df_final2['created_dt']=pd.to_datetime(df_final2['created_at']\
                                           , errors='coerce')
    df_final2['create_round']=df_final2["created_dt"].dt.round('D')
    df_final2['created_dt']=df_final2['created_dt'].dt.strftime('%Y-%m-%d %H:%M')
    df_final2['updated_dt']=pd.to_datetime(df_final2['updated_at']\
                                           , errors='coerce')
    df_final2['updated_dt']=df_final2['updated_dt'].dt.strftime('%Y-%m-%d %H:%M')
    df_final2['create_round']=df_final2['create_round'].dt.strftime('%Y-%m-%d')
    df_final2['ticket_id']=df_final2['url'].str.\
        extract('tickets\/([0-9]+)')
    df_final2['ticket_url']='https://citybase.zendesk.com/agent/tickets/'+df_final2['ticket_id'].map(str)
    df_final2['kiosk_extracted1']=df_final2['description'].str.\
        extract(':[0-9]{2}:[0-9]{2}\s{1}([0-9a-z]+)\s{1}chromium\.log')
    df_final2['kiosk_extracted2']=df_final2['description'].str.\
        extract(':[0-9]{2}:[0-9]{2}\s{1}([0-9a-z]+)\s{1}production\.log')
    df_final2['kiosk_extracted_secondary_id1']=df_final2['description'].str.\
        extract('\s{1}\(([0-9a-z]+):proc\.num')
    df_final2['kiosk_extracted_secondary_id2']=df_final2['description'].str.\
        extract('\s{1}\(([0-9a-z]+):net\.tcp')
    df_final2['kiosk_extracted_secondary_id1']=df_final2['kiosk_extracted_secondary_id1']\
        .fillna(df_final2['kiosk_extracted_secondary_id2'])
    df_final2['kiosk_extracted1']=df_final2['kiosk_extracted1']\
        .fillna(df_final2['kiosk_extracted2'])
    df_final2['kiosk_extracted1']=df_final2['kiosk_extracted1']\
        .fillna(df_final2['kiosk_extracted_secondary_id1'])
    df_final2['description'] = df_final2['description'].str.replace('\t',' ')
    df_final2['description'] = df_final2['description'].str.replace\
        ('[^A-Za-z0-9\s\.,()"\[\]-]+','')
    return df_final2


def merge_csv (transformed_df,csv_file):
    df_kiosk_match=pd.read_csv(csv_file)
    df_kiosk_match['periscope_kiosks'] = df_kiosk_match['periscope_kiosks'].astype('str')
    mask = (df_kiosk_match['periscope_kiosks'].str.len() >2)
    df_kiosk_match = df_kiosk_match.loc[mask]
    df_kiosk_match['periscope_kiosks']=df_kiosk_match['periscope_kiosks'].str.lower()
    df_kiosk_match['Bills_kiosk_id2']=df_kiosk_match['Bills_kiosk_id2'].str.lower()
    df2=df_kiosk_match[['Bills_kiosk_id2','periscope_kiosks']]
    df2.rename(columns = {'periscope_kiosks':'kiosk_code_2',\
        'Bills_kiosk_id2':"kiosk_extracted1"}, inplace = True)
    df_merge=pd.merge(transformed_df, df2, on='kiosk_extracted1', how='left')
    df_merge=df_merge.drop_duplicates(subset='url')

    df_merge['kiosk_code']=np.where(df_merge['kiosk_normalized'].str.len()>1,df_merge['kiosk_normalized'],\
                                     df_merge['kiosk_code_2'])
    df_merge['kiosk_code']=np.where(df_merge['kiosk_code'].str.len()>3,df_merge['kiosk_code'],\
                                     np.nan)
    df_kiosk_match.rename(columns = {'periscope_kiosks':'kiosk_code'}, inplace = True)
    df_merge2=pd.merge(df_merge, df_kiosk_match, on='kiosk_code', how='left')
    return df_merge2

already_created_connection='zendesk_connection_FINAL4'
unix_time=convert_time_delta_to_unix(unit=time_back_unit, time_back=time_back_val)
path="/api/v2/incremental/tickets.json?start_time={}".format(unix_time)

json_list_response=zendesk_hook_call(already_created_connection,path_str=path)
raw_df=transform(json_list_response)
merged_transformed_df=merge_csv(raw_df,'kiosk_match.csv')

#FINAL UPLOAD TO S3 BUCKET
already_created_s3_conn='zendesk_s3_connection'
bucket_name='data-science-etl-us-east-1'
target_filename='{}_zendesk_upload.csv'.format(today_date_hr_str)
key_name='{}_zendesk_upload'.format(today_date_hr_str)
sub_folder_str='zendesk'

os.path.join(sub_folder_str, key_name)
s3_hook = S3Hook(aws_conn_id=already_created_s3_conn)
s3_conn = s3_hook.get_connection(already_created_s3_conn)
s3_resource = s3_hook.get_conn()
s3_resource.upload_file(target_filename,Key=key_name,Bucket=bucket_name)
