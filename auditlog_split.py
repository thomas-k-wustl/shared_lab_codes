#!/usr/bin/env python
# coding: utf-8
"""
Created on April 26 2024

This script contains a function to determine & assign a unique identifier for each "session" of EHR activity.
"Session" can be defined in either of two ways:
    1. "Session" : consecutive set of EHR actions with a gap <= 5min
    2. "Patient-Session" : consecutive set of EHR actions with a gap <= 5min, for a single unique patient

Updated June 7 2024
* Uses ACCESS_TIME instead of ACCESS_INSTANT

@author: Seunghwan (Nigel) Kim
@email: seunghwan.kim@wustl.edu
"""
import numpy as np
import pandas as pd

pd.set_option('future.no_silent_downcasting', True)


def calc_sessions(df, timestamp_col, cap_gap_minutes=5, pat_session=False):
    # print('Splitting audit log table into sessions...')
    # 5min cap gap session interval
    SESSION_INTERVAL = cap_gap_minutes * 60

    # Timedelta_style1: diff(current, previous)
    time_deltas = df.loc[:, timestamp_col].diff(periods=1)
    # Timedelta_style2: diff(current, next)
    # time_deltas = df.loc[:, 'ACCESS_INSTANT'].diff(periods=-1)*-1

    # Must impute first row's timedelta with an artificial value > SESSION_INTERVAL to denote session start
    time_deltas.fillna(999, inplace=True)

    # Assign split points (any gap > 5 min will be assigned NULL timedelta value)
    df['TIME_DELTA'] = np.where((time_deltas > SESSION_INTERVAL), np.nan, time_deltas)

    # Use split points to assign unique session ID formatted as "{USER_ID}_{ACCESS_TIME}" (each split point is the starting point of a session, according our timedelta definition above)
    if pat_session:
        # if using a tighter 'patient-session' definition (i.e., consecutive actions with a gap<=5min, on a single patient)
        ## define additional split point criteria (patient switching transition)
        ## create column 'pat_curr' to track the most recent PAT_ID that has been touched
        df['pat_curr'] = df['PAT_ID']
        ## forward-fill 'pat_curr' to track the most recent PAT_ID  that has been touched
        df.loc[df['TIME_DELTA'].isnull(), 'pat_curr'] = 'SESSION_START'
        df['pat_curr'].ffill(inplace=True)

        def catch_pat_switch(df):
            '''
            Mark a flag when either a 1)session starts or 2)patient switch happens
            '''
            df.loc[(df['pat_curr'] == 'SESSION_START') |
                   ((df['pat_curr'] != df['pat_curr'].shift(1)) & (df['pat_curr'].shift(1) != 'SESSION_START')),
            'pat_switch'] = 1
            # n_pat_switch = len(df[df['pat_switch']==1])
            return df  # , n_pat_switch

        df = catch_pat_switch(df)
        df.loc[df['pat_switch'] == 1, 'session_ID'] = df['USER_ID'] + "_" + df['ACCESS_TIME'].astype(str)
    else:
        # if using a traditional 'session' definition (i.e., consecutive actions with a gap<=5min)
        # print(df.head(1))
        # print(f"df['USER_ID'] data type: {df['USER_ID'].dtype}, val: {df.USER_ID.values[0]}")
        # print(f"df['ACCESS_TIME'] data type: {df['ACCESS_TIME'].dtype}, val: {df.ACCESS_TIME.values[0]}")
        df.loc[df['TIME_DELTA'].isnull(), 'session_ID'] = df['USER_ID'].astype(str) + "_" + df['ACCESS_TIME'].astype(
            str)

    # Fill in 'NULL' for NaN values and replace non-null values with processed ones
    df['TIME_DELTA'] = df['TIME_DELTA'].apply(lambda x: 'NULL' if pd.isnull(x) else str(int(x)))

    # Forward-fill to fill in session ID's to the actions in the same session
    df['session_ID'] = df['session_ID'].ffill()  #.infer_objects(copy=False)

    # if pat_session:
    #     print(f"Unique count of patient-sessions: {df.session_ID.nunique()}")
    # else:
    #     print(f"Unique count of sessions: {df.session_ID.nunique()}")

    return df
