#!/usr/bin/env python
# coding: utf-8
"""
Created on July 5 2023

Tests audit log data sets for timestamp mismatches between ACESS_INSTANT and ACCESS_TIME attributes.

@author: Seunghwan (Nigel) Kim
@email: seunghwan.kim@wustl.edu

"""
# In[]:
import pandas as pd
import datetime
import os
import time
import argparse
import sys
import warnings
warnings.filterwarnings("ignore")
# In[]:
def explore_timestamp_mismatch(df):
    global metric_dict, metric_id_missing_flag
    # Making sure the ACCESS_TIME column is in datetime64 type
    df['ACCESS_TIME'] = pd.to_datetime(df['ACCESS_TIME'])
    # Calculate the difference between Epic's epoch time (1840/12/31) and Unix epoch time (1970/1/1)
    epoch_diff = (datetime.datetime(1970, 1, 1) - datetime.datetime(1840, 12, 31))
    # Convert ACCESS_INSTANT to datetime after incorporating Epic's epoch time (1840/12/31) and then converting UTC(+0) to CST(-6) timezone
    df['ACCESS_INSTANT_to_datetime_dtl'] = (pd.to_datetime(df['ACCESS_INSTANT'], unit='s') - epoch_diff) + datetime.timedelta(hours= -6)
    df['ACCESS_INSTANT_to_datetime'] = pd.to_datetime(df['ACCESS_INSTANT_to_datetime_dtl'].dt.strftime('%Y-%m-%d %H:%M:%S'))
    
    ### ---MUST REVISE CODE HERE TO INCORPORATE DAYLIGHT SAVING TIME FROM ADDITIONAL CALENDAR YEARS. CURRENT: ONLY 2019 ---------
    # In Epic, Daylight Saving Time doesn't seem to be applied ACCESS_INSTANTs. It seems that it was only appied to ACCESS_TIME.
    # Therefore, subtracting 1 hour to the ACCESS_TIME values that were between the Daylight Saving Time period in 2019 (3/10/2019 2am ~ 11/3/2019 2am) to compare with ACCESS_INSTANTs.
    df.loc[(df['ACCESS_TIME']>=datetime.datetime(2019, 3, 10, 2, 0, 0)) & (df['ACCESS_TIME']<=datetime.datetime(2019, 11, 3, 2, 0, 0)), 'ACCESS_TIME'] \
    = df.loc[(df['ACCESS_TIME']>=datetime.datetime(2019, 3, 10, 2, 0, 0)) & (df['ACCESS_TIME']<=datetime.datetime(2019, 11, 3, 2, 0, 0)), 'ACCESS_TIME'] - datetime.timedelta(hours=1)
    ### -------------------------------------------------------------------------------------------------------------------------
    
    # Create a column that marks the presence of timestamp mismatch between ACCESS_INSTANT and ACCESS_TIME
    df['time_mismatch'] = (df['ACCESS_INSTANT_to_datetime'] != df['ACCESS_TIME'])
    # Create a column that calculates the magnitude of timestamp mismatch 
    df['mismatch_amount'] = df['ACCESS_INSTANT_to_datetime_dtl'] - df['ACCESS_TIME']
    print('{}/{} audit log events have mismatching timestamps'.format(len(df[(df['time_mismatch']==True)]), len(df)))
    if len(df[(df['time_mismatch']==True)]) > 0:
        print('Magnitudes of mismatch:\n',
              df.loc[df['time_mismatch']==True, 'mismatch_amount'].value_counts().index.sort_values())
        print('Maximum delay observed:{}'.format(df.loc[df['time_mismatch']==True, 'mismatch_amount'].max()))
        print('Minimum delay observed:{}'.format(df.loc[df['time_mismatch']==True, 'mismatch_amount'].min()))
        print('Average delay observed:{}'.format(df.loc[df['time_mismatch']==True, 'mismatch_amount'].mean()))
        
    if metric_id_missing_flag == 1:
        print('Number of unique METRIC_NAMEs that has mismatching time: {}'.format(len(df.loc[df['time_mismatch']==True, 'METRIC_NAME'].value_counts())))
        target_metric_ids_w_names = df.loc[df['time_mismatch']==True, 'METRIC_NAME'].value_counts().reset_index()
        target_metric_ids_w_names.columns = ['METRIC_NAME', 'Frequency']
    else:
        print('Number of unique METRIC_IDs that has mismatching time: {}'.format(len(df.loc[df['time_mismatch']==True, 'METRIC_ID'].value_counts())))
        # Create a table with a breakdown of METRIC_ID frequencies for the events with timestamp mismatch
        target_metric_ids = df.loc[df['time_mismatch']==True, 'METRIC_ID'].value_counts().reset_index()
        target_metric_ids.columns = ['METRIC_ID', 'Frequency']
        # Attach METRIC_NAMES using a dictionary, for better readability
        target_metric_ids_w_names = target_metric_ids.merge(metric_dict[['METRIC_ID', 'METRIC_NAME']], on='METRIC_ID', how='left')

    return target_metric_ids_w_names

# In[ ]:
if __name__ == '__main__':
    '''
    Run the code in shell:
    python3 timestamp_alignment_test.py --os OS --filepath FILEPATH
    '''
    parser = argparse.ArgumentParser(description='Explore mismatch between EHR audit log ACCESS_INSTANTs and ACCESS_TIMEs.')
    parser.add_argument('--os', help="your current machine's operating system (eg, windows, mac)", default='none')
    parser.add_argument('--filepath', help='directory containing subfolders with raw audit log data', default='none')
    parser.add_argument('--metric_id_missing', help='is METRIC_ID column missing in your data?', default='no')
    
    args = parser.parse_args()
    
    # Specify what your operating system is
    if args.os == "windows": # windows OS
        root_dir = os.path.join("Z:", "Active")
    elif args.os == "mac": # mac OS
        root_dir = os.path.join("/Volumes", "Active")
    else:
        sys.exit("You MUST specify your operating system environment. Example: windows, mac")

    # Specify where the audit log data is
    if args.filepath == "none":
        sys.exit("You MUST specify your data's filepath within thomas.k RIS data storage. Example: nlm_telemedicine_ehr_logs/raw_data/AL_first1mil.csv")
    else:
        data_dir = os.path.join(root_dir, args.filepath)
    
    metric_id_missing_flag = 0
    # If METRIC_ID column is missing in your data, flag is turned on
    if args.metric_id_missing == 'yes':
        metric_id_missing_flag = 1
    
    filetype = data_dir.split('.')[-1]
    print("Your data file is in a '{}' format".format(filetype))
    
    startTime = time.time()
    ### ==========================================================================================================================
    print('Loading raw audit logs from your data file...')
    ### --- Examples imported from Nigel ----------------------------------------------
    # # NLM outpatient data set (sample of 1 million rows)
    # target_data = pd.read_csv(os.path.join(root_dir, 'nlm_telemedicine_ehr_logs/raw_data/AL_first1mil.csv'))
    # # SICU inpatient data set (sample of 1 provider)
    # target_data = pd.read_csv(os.path.join(root_dir, 'icu_ehr_logs/raw_data/2019/4604/access_log_complete.csv'))
    # # Secure Chat data set (sample of 1 provider)
    # target_data = pd.read_parquet(os.path.join(root_dir, 'secure_chat_messaging/Audit_logs/Audit_logs_OLD/2369.parquet', engine='auto'))
    # target_data = pd.read_parquet(os.path.join(root_dir, 'secure_chat_messaging/Audit_logs/Audit_logs_OLD/M84624.parquet', engine='auto')
    # target_data = pd.read_parquet(os.path.join(root_dir, 'secure_chat_messaging/Audit_logs/Audit_logs_OLD/U0002120.parquet', engine='auto')
    # target_data = pd.read_parquet(os.path.join(root_dir, 'secure_chat_messaging/Audit_logs/Audit_logs_OLD/M151757.parquet', engine='auto')
    ### -------------------------------------------------------------------------------
    ### Importing YOUR dataset...
    if filetype == 'csv':
        target_data = pd.read_csv(data_dir)
    elif filetype == 'parquet':
        target_data = pd.read_parquet(data_dir, engine='auto')
    elif filetype == 'pickle':
        import pickle
        with open(data_dir, 'rb') as input_file:
            target_data = pickle.load(input_file)
    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))
    metric_dict = pd.read_excel(os.path.join(root_dir, 'nlm_telemedicine_ehr_logs/summary_data/metric_dictionary/ACCESS_LOG_METRIC_DICTIONARY.xlsx'))
    ### ==========================================================================================================================
    
    results = explore_timestamp_mismatch(target_data)
    
    # Save
    print("Saving results...")
    results.to_excel(os.path.join(root_dir, 'timestamp_mismatch/results/'\
                                  +args.filepath.split('/')[0]+'_'+args.filepath.split('.')[0].split('/')[-1]+'.xlsx'))

    executionTime = (time.time() - startTime)
    print('Total execution time in seconds: ' + str(executionTime))


